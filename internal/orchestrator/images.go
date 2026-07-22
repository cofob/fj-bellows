package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/hstern/fj-bellows/internal/bootstrap"
	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/storage"
)

const (
	imageReconcileInterval = 5 * time.Minute
	mutationDeleteSnapshot = "delete_snapshot"
	resetModeSnapshot      = "snapshot"
)

//nolint:gocyclo // Crash recovery deliberately classifies durable and provider builder states in one pass.
func (o *Orchestrator) recoverOrphanBuilders(ctx context.Context) error {
	if o.cfg.ResetMode != resetModeSnapshot || o.builderCount() != 0 {
		return nil
	}
	now := o.now()
	o.mu.Lock()
	due := o.lastBuilderCheck.IsZero() || now.Sub(o.lastBuilderCheck) >= imageReconcileInterval
	if due {
		o.lastBuilderCheck = now
	}
	o.mu.Unlock()
	if !due {
		return nil
	}
	providerWithBuilders, ok := o.prov.(provider.BuilderProvider)
	if !ok {
		return nil
	}
	builders, err := providerWithBuilders.ListBuilders(ctx, o.cfg.Tag)
	if err != nil {
		return fmt.Errorf("list managed image builders: %w", err)
	}
	if len(builders) == 0 {
		return nil
	}
	resources, generations, err := o.storageResources(ctx)
	if err != nil {
		return err
	}
	for _, builder := range builders {
		resource := resources[builder.ID]
		if resource.ID == 0 {
			resource = resources["name:"+builder.Name]
		}
		generation := generations[resource.ID]
		if o.store != nil && resource.ID != 0 && resource.ExternalID == "" {
			generation, err = o.activateStoredResource(ctx, resource, builder)
			if err != nil {
				return err
			}
			resource.ExternalID = builder.ID
			resource.ProviderCreatedAt = builder.CreatedAt
		} else if o.store != nil && resource.ID == 0 {
			resource, generation, err = o.adoptStoredResource(ctx, builder)
			if err != nil {
				return err
			}
		}
		node := Node{
			InstanceID: builder.ID, ResourceID: resource.ID, GenerationID: generation.ID,
			PriceQuoteID: resource.PriceQuoteID, IP: builder.IPv4, VPCIP: builder.VPCIPv4,
			CreatedAt: builder.CreatedAt,
		}
		node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseImageBuild, 0, o.now())
		operationID, err := o.beginStoredMutation(ctx, "destroy_builder", node)
		if err != nil {
			return err
		}
		if err := o.prov.Destroy(ctx, builder.ID); err != nil {
			o.finishStoredMutation(ctx, operationID, builder.ID, err.Error(), false)
			o.finishStoredPhase(ctx, node.PhaseID, "failed", err.Error(), o.now())
			return fmt.Errorf("destroy orphan builder %s: %w", builder.ID, err)
		}
		o.finishStoredMutation(ctx, operationID, builder.ID, "", true)
		o.finishStoredPhase(ctx, node.PhaseID, "recovered_destroy", "", o.now())
		o.recordResourceCost(ctx, node, storage.CostImageBuilder, o.now())
		if o.store != nil && node.GenerationID != 0 {
			_ = o.store.SetGenerationState(ctx, node.GenerationID, storage.GenerationClosed, o.now())
		}
		if o.store != nil && node.ResourceID != 0 {
			if err := o.store.CloseResource(ctx, node.ResourceID, storage.ResourceClosed,
				"orphan_builder_recovered", o.now()); err != nil {
				return err
			}
		}
		o.emit("snapshot_builder_reaped", map[string]string{attrID: builder.ID})
	}
	return nil
}

// ensureManagedImage performs the cheap discovery/launch half of managed
// image reconciliation. The expensive build runs in the orchestrator wait
// group and counts against max_instances, but never against future worker
// capacity. Queued work takes precedence over starting a new builder.
//
//nolint:gocyclo // Capability checks and build scheduling are kept together to preserve the single-launch invariant.
func (o *Orchestrator) ensureManagedImage(ctx context.Context, hasDemand bool) {
	if o.cfg.ResetMode != resetModeSnapshot {
		return
	}
	images, ok := o.prov.(provider.ManagedImageProvider)
	if !ok {
		return
	}
	runtime := o.runtimeConfig()
	preparer, ok := o.disp.(ImagePreparer)
	if !ok {
		return
	}
	now := o.now()
	o.mu.Lock()
	due := o.lastImageCheck.IsZero() || now.Sub(o.lastImageCheck) >= imageReconcileInterval || o.managedImage == ""
	if due {
		o.lastImageCheck = now
	}
	o.mu.Unlock()
	if !due {
		return
	}
	fingerprint := o.imageFingerprint()
	owned, err := images.ListImages(ctx, o.cfg.Tag)
	if err != nil {
		o.log.Error("list managed images", "tier", o.cfg.Tier, "err", err)
		return
	}
	if err := o.recoverStoredSnapshots(ctx, owned); err != nil {
		o.storageError("recover managed images", err)
		return
	}
	matching := make([]provider.ManagedImage, 0, len(owned))
	for _, img := range owned {
		if img.Fingerprint == fingerprint {
			matching = append(matching, img)
		}
	}
	if len(matching) > 0 {
		sort.Slice(matching, func(i, j int) bool { return matching[i].CreatedAt.After(matching[j].CreatedAt) })
		if err := o.activateStoredSnapshot(ctx, matching[0]); err != nil {
			o.storageError("adopt managed image", err)
			return
		}
		o.SetManagedImage(matching[0].ID)
		o.cleanupStaleImages(ctx, images, owned, matching[0].ID)
		return
	}
	o.SetManagedImage("")
	if o.builderCount() != 0 {
		return
	}
	// A warm tier prepares its image eagerly. A cold tier (warm_instances=0)
	// stays truly scale-to-zero and starts the builder only when the first job
	// arrives; with max_instances=1 that job waits for the image, while larger
	// tiers may serve cold capacity concurrently without starving rotation.
	if !hasDemand && runtime.WarmInstances == 0 {
		return
	}
	if o.pool.Len()+o.pendingCount()+o.builderCount() >= runtime.MaxScale {
		return
	}
	o.incBuilders()
	o.wg.Go(func() {
		defer o.decBuilders()
		o.buildManagedImage(ctx, images, preparer, fingerprint)
	})
}

func (o *Orchestrator) imageFingerprint() string {
	if o.cfg.BootstrapFingerprint != "" {
		return o.cfg.BootstrapFingerprint
	}
	h := sha256.New()
	runtime := o.runtimeConfig()
	_, _ = fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s", o.cfg.Driver, o.cfg.InstanceType,
		runtime.RunnerVersion, o.cfg.FJBAgentDownloadURL, o.cfg.ReadyFile)
	return hex.EncodeToString(h.Sum(nil))
}

//nolint:gocyclo,funlen // Each build stage has distinct durable compensation and must remain visibly ordered.
func (o *Orchestrator) buildManagedImage(
	ctx context.Context,
	images provider.ManagedImageProvider,
	preparer ImagePreparer,
	fingerprint string,
) {
	runtime := o.runtimeConfig()
	pinner, canPin := o.disp.(HostKeyPinner)
	var hostPriv string
	var hostPub ssh.PublicKey
	if canPin {
		var err error
		hostPriv, hostPub, err = generateHostKey()
		if err != nil {
			o.log.Error("generate builder host key", "err", err)
			return
		}
	}
	agentToken := ""
	if o.cfg.FJBAgentDownloadURL != "" {
		// Render requires a token whenever it installs the agent. This value is
		// intentionally useless and is scrubbed before the disk is captured.
		agentToken = "snapshot-builder-placeholder"
	}
	userData, err := bootstrap.Render(bootstrap.Params{
		RunnerVersion:       runtime.RunnerVersion,
		ReadyFile:           o.cfg.ReadyFile,
		HostPrivateKey:      hostPriv,
		FJBAgentDownloadURL: o.cfg.FJBAgentDownloadURL,
		FJBAgentToken:       agentToken,
	})
	if err != nil {
		o.log.Error("render image builder", "err", err)
		return
	}
	name := o.cfg.Tag + "-image-" + shortID()
	resource, phase, err := o.beginStoredResource(ctx, name, name)
	if err != nil {
		return
	}
	inst, err := o.prov.Provision(ctx, provider.Spec{
		Tag:           o.cfg.Tag,
		Name:          name,
		Role:          "builder",
		InstanceType:  o.cfg.InstanceType,
		UserData:      userData,
		AuthorizedKey: o.cfg.AuthorizedKey,
		Labels:        o.cfg.Labels,
	})
	if err != nil {
		o.failStoredProvision(ctx, resource, phase.ID, "provider_create_builder")
		o.log.Error("provision image builder", "err", err)
		o.emit("snapshot_build_failed", map[string]string{attrStage: "provision"})
		return
	}
	generation, err := o.activateStoredResource(ctx, resource, inst)
	if err != nil {
		o.cleanupFailedActivation(ctx, resource, phase.ID, inst, "storage_activation_builder")
		return
	}
	builderNode := Node{
		InstanceID: inst.ID, ResourceID: resource.ID, GenerationID: generation.ID,
		PriceQuoteID: resource.PriceQuoteID, IP: inst.IPv4, VPCIP: inst.VPCIPv4,
		CreatedAt: inst.CreatedAt, PhaseID: phase.ID,
	}
	reusedAsWorker := false
	builderPhaseFinished := false
	defer func() {
		if reusedAsWorker {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		op, opErr := o.beginStoredMutation(cleanupCtx, "destroy_builder", builderNode)
		if opErr != nil {
			o.log.Error("record image builder destroy", "id", inst.ID, "err", opErr)
			o.pool.SetState(inst.ID, StateDraining)
			return
		}
		if derr := o.prov.Destroy(cleanupCtx, inst.ID); derr != nil {
			o.finishStoredMutation(cleanupCtx, op, inst.ID, "provider_destroy", false)
			o.log.Error("destroy image builder", "id", inst.ID, "err", derr)
			o.pool.SetState(inst.ID, StateDraining)
			return
		}
		o.finishStoredMutation(cleanupCtx, op, inst.ID, "", true)
		if o.store != nil && generation.ID != 0 {
			_ = o.store.SetGenerationState(cleanupCtx, generation.ID, storage.GenerationClosed, o.now())
		}
		if !builderPhaseFinished {
			o.finishStoredPhase(cleanupCtx, builderNode.PhaseID, "complete", "", o.now())
		}
		o.recordResourceCost(cleanupCtx, builderNode, storage.CostImageBuilder, o.now())
		if o.store != nil && resource.ID != 0 {
			if closeErr := o.store.CloseResource(cleanupCtx, resource.ID, storage.ResourceClosed, "image_builder_complete", o.now()); closeErr != nil {
				o.storageError("close image builder", closeErr)
			}
		}
		o.pool.Delete(inst.ID)
	}()
	o.emit("snapshot_build_started", map[string]string{attrID: inst.ID, attrIP: inst.IPv4})
	if canPin {
		pinner.PinHostKey(inst.IPv4, hostPub)
	}
	addr := o.addrForInstance(inst.IPv4, inst.VPCIPv4)
	if err := o.disp.WaitReady(ctx, inst.ID, addr); err != nil {
		if o.store != nil && generation.ID != 0 {
			_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
		}
		o.log.Error("image builder readiness", "id", inst.ID, "err", err)
		o.emit("snapshot_build_failed", map[string]string{attrStage: "readiness", attrID: inst.ID})
		return
	}
	if o.store != nil && generation.ID != 0 {
		if err := o.store.SetGenerationState(ctx, generation.ID, storage.GenerationReady, o.now()); err != nil {
			o.storageError("ready image builder", err)
			return
		}
	}
	o.finishStoredPhase(ctx, builderNode.PhaseID, "ready", "", o.now())
	builderNode.PhaseID = o.startStoredPhase(ctx, builderNode, storage.PhaseImageBuild, 0, o.now())
	if err := preparer.PrepareImage(ctx, inst.ID, addr); err != nil {
		o.log.Error("prepare image builder", "id", inst.ID, "err", err)
		o.emit("snapshot_build_failed", map[string]string{attrStage: "sysprep", attrID: inst.ID})
		return
	}
	snapshot, err := o.beginStoredSnapshot(ctx, resource.ID, fingerprint)
	if err != nil {
		return
	}
	img, err := images.CreateImage(ctx, provider.ImageSpec{
		Tag:              o.cfg.Tag,
		Name:             o.cfg.Tag + "-golden-" + fingerprint[:12],
		SourceInstanceID: inst.ID,
		Fingerprint:      fingerprint,
	})
	if err != nil {
		o.failStoredSnapshot(ctx, snapshot, err.Error())
		o.log.Error("create managed image", "builder_id", inst.ID, "err", err)
		o.emit("snapshot_build_failed", map[string]string{attrStage: resetModeSnapshot, attrID: inst.ID})
		return
	}
	if o.store != nil && snapshot.ID != 0 {
		if err := o.store.ActivateSnapshot(ctx, snapshot.ID, img.ID, img.SizeBytes, o.now()); err != nil {
			o.storageError("activate snapshot", err)
			// The provider image exists but cannot be safely selected without a
			// durable active record. Leave it labelled for the next reconcile.
			return
		}
	}
	o.SetManagedImage(img.ID)
	o.emit("snapshot_activated", map[string]string{"image_id": img.ID, "fingerprint": fingerprint})
	// Hetzner (and any future provider exposing BuilderPromoter) can retain the
	// allocation that paid for this cold build. Mark its filesystem unclean
	// before changing the provider role: after a crash, reconcile will drain a
	// promoted server unless the subsequent rebuild durably reaches Ready.
	if promoter, ok := o.prov.(provider.BuilderPromoter); ok {
		o.finishStoredPhase(ctx, builderNode.PhaseID, "snapshot_created", "", o.now())
		builderPhaseFinished = true
		if o.promoteImageBuilder(ctx, builderNode, generation, promoter, img.ID) {
			reusedAsWorker = true
		}
	}
	owned, err := images.ListImages(ctx, o.cfg.Tag)
	if err == nil {
		o.cleanupStaleImages(ctx, images, owned, img.ID)
	}
}

// promoteImageBuilder converts the just-snapshotted builder into the first
// clean worker without opening a second paid allocation. resetWorker owns the
// normal rebuild/readiness transition once the provider role is changed.
func (o *Orchestrator) promoteImageBuilder(
	ctx context.Context,
	node Node,
	generation storage.Generation,
	promoter provider.BuilderPromoter,
	imageID string,
) bool {
	if o.store != nil && generation.ID != 0 {
		if err := o.store.SetGenerationState(ctx, generation.ID, storage.GenerationDirty, o.now()); err != nil {
			o.storageError("mark image builder unclean", err)
			return false
		}
		if err := o.store.SetResourceState(ctx, node.ResourceID, storage.ResourceResetting, o.now()); err != nil {
			o.storageError("prepare image builder promotion", err)
			return false
		}
	}
	// Publish a non-dispatchable pool entry before the provider role changes.
	// A reconcile racing the API update will therefore recognize this exact
	// allocation instead of independently adopting it in the tiny interval
	// between the label update and the reset.
	node.State = StateResetting
	o.pool.Put(&node)
	if err := promoter.PromoteBuilder(ctx, node.InstanceID, o.cfg.Tag); err != nil {
		o.log.Error("promote image builder", "id", node.InstanceID, "err", err)
		o.emit("snapshot_builder_promotion_failed", map[string]string{attrID: node.InstanceID})
		return false
	}
	o.emit("snapshot_builder_promoted", map[string]string{attrID: node.InstanceID})
	acquired := o.acquireManagedImage()
	if acquired == "" || acquired != imageID {
		if acquired != "" {
			o.releaseImage(acquired)
		}
		o.pool.SetState(node.InstanceID, StateDraining)
		return false
	}
	resetter, ok := o.prov.(provider.Resetter)
	if !ok || !o.resetWorker(ctx, node, resetter, acquired) {
		if !ok {
			o.releaseImage(acquired)
		}
		return false
	}
	o.recordIntervalCost(ctx, node, storage.CostImageBuilder, node.CreatedAt, o.now())
	return true
}

//nolint:gocyclo // Image deletion coordinates provider truth, mutations, costs, and snapshot state atomically.
func (o *Orchestrator) cleanupStaleImages(
	ctx context.Context,
	images provider.ManagedImageProvider,
	owned []provider.ManagedImage,
	activeID string,
) {
	if o.store != nil {
		defer func() {
			for _, image := range owned {
				if image.ID == activeID {
					if err := o.activateStoredSnapshot(ctx, image); err != nil {
						o.storageError("restore active snapshot", err)
					}
					return
				}
			}
		}()
	}
	for _, img := range owned {
		if img.ID == activeID {
			continue
		}
		if o.imageInUse(img.ID) {
			continue
		}
		snapshot, err := o.ensureStoredSnapshot(ctx, img)
		if err != nil {
			o.storageError("record stale snapshot", err)
			continue
		}
		operationID := ""
		if o.store != nil && snapshot.ID != 0 {
			if err := o.store.SetSnapshotState(ctx, snapshot.ID, storage.SnapshotDeleting, "", o.now()); err != nil {
				o.storageError("mark snapshot deleting", err)
				continue
			}
			operationID = fmt.Sprintf("delete-snapshot-%s-%s", img.ID, shortID())
			if err := o.store.BeginMutation(ctx, storage.Mutation{
				OperationID: operationID, Kind: mutationDeleteSnapshot, Provider: o.cfg.ProviderName,
				Tier: o.cfg.Tier, SnapshotID: snapshot.ID, State: storage.MutationPending,
				CreatedAt: o.now(),
			}); err != nil {
				o.storageError("begin snapshot deletion", err)
				continue
			}
		}
		if err := images.DeleteImage(ctx, img.ID); err != nil {
			if o.store != nil && operationID != "" {
				_ = o.store.FinishMutation(ctx, operationID, storage.MutationFailed, img.ID, err.Error(), o.now())
				_ = o.store.SetSnapshotState(ctx, snapshot.ID, storage.SnapshotStale, err.Error(), o.now())
			}
			o.log.Warn("delete stale managed image", "image_id", img.ID, "err", err)
			continue
		}
		if o.store != nil && snapshot.ID != 0 {
			_ = o.store.FinishMutation(ctx, operationID, storage.MutationSucceeded, img.ID, "", o.now())
			o.recordSnapshotStorageCost(ctx, snapshot, o.now())
			if err := o.store.SetSnapshotState(ctx, snapshot.ID, storage.SnapshotDeleted, "", o.now()); err != nil {
				o.storageError("mark snapshot deleted", err)
			}
		}
		o.emit("snapshot_deleted", map[string]string{"image_id": img.ID})
	}
}

// recoverStoredSnapshots resolves managed-image mutations left pending by a
// crash against provider truth. Provider images are authoritative for
// existence; the database remains authoritative for safe activation.
//
//nolint:gocyclo // The recovery state table is intentionally explicit so no snapshot state falls through implicitly.
func (o *Orchestrator) recoverStoredSnapshots(ctx context.Context, owned []provider.ManagedImage) error {
	if o.store == nil {
		return nil
	}
	records, err := o.store.Snapshots(ctx, storage.SnapshotFilter{
		Provider: o.cfg.ProviderName,
		Tier:     o.cfg.Tier,
	})
	if err != nil {
		return err
	}
	pending, err := o.store.PendingMutations(ctx)
	if err != nil {
		return err
	}
	pendingDeletes := make(map[int64][]storage.Mutation)
	for _, mutation := range pending {
		if mutation.Provider == o.cfg.ProviderName && mutation.Tier == o.cfg.Tier &&
			mutation.Kind == mutationDeleteSnapshot {
			pendingDeletes[mutation.SnapshotID] = append(pendingDeletes[mutation.SnapshotID], mutation)
		}
	}

	byID := make(map[string]provider.ManagedImage, len(owned))
	byKey := make(map[string]provider.ManagedImage, len(owned))
	for _, image := range owned {
		byID[image.ID] = image
		key := image.Name + "\x00" + image.Fingerprint
		if current, ok := byKey[key]; !ok || image.CreatedAt.After(current.CreatedAt) {
			byKey[key] = image
		}
	}
	for _, record := range records {
		image, exists := byID[record.ExternalID]
		if !exists && record.ExternalID == "" {
			image, exists = byKey[record.Name+"\x00"+record.Fingerprint]
		}
		switch record.State {
		case storage.SnapshotBuilding:
			if exists {
				if err := o.store.ActivateSnapshot(ctx, record.ID, image.ID, image.SizeBytes, o.now()); err != nil {
					return err
				}
				continue
			}
			// The row belonging to an image build currently running in this
			// process is not orphaned. With no builder left, provider absence
			// proves that the interrupted create yielded no usable image.
			if o.builderCount() == 0 {
				if err := o.store.SetSnapshotState(ctx, record.ID, storage.SnapshotFailed,
					"provider image missing during recovery", o.now()); err != nil {
					return err
				}
			}
		case storage.SnapshotDeleting:
			if exists {
				for _, mutation := range pendingDeletes[record.ID] {
					if err := o.store.FinishMutation(ctx, mutation.OperationID, storage.MutationFailed,
						image.ID, "provider image still exists; deletion will be retried", o.now()); err != nil {
						return err
					}
				}
				if err := o.store.SetSnapshotState(ctx, record.ID, storage.SnapshotStale,
					"recovered incomplete deletion", o.now()); err != nil {
					return err
				}
				continue
			}
			o.recordSnapshotStorageCost(ctx, record, o.now())
			if err := o.store.SetSnapshotState(ctx, record.ID, storage.SnapshotDeleted,
				"provider image absent during recovery", o.now()); err != nil {
				return err
			}
			for _, mutation := range pendingDeletes[record.ID] {
				if err := o.store.FinishMutation(ctx, mutation.OperationID, storage.MutationSucceeded,
					record.ExternalID, "provider image absent during recovery", o.now()); err != nil {
					return err
				}
			}
		case storage.SnapshotActive, storage.SnapshotStale:
			if !exists && record.ExternalID != "" {
				o.recordSnapshotStorageCost(ctx, record, o.now())
				if err := o.store.SetSnapshotState(ctx, record.ID, storage.SnapshotDeleted,
					"provider image disappeared", o.now()); err != nil {
					return err
				}
			}
		case storage.SnapshotDeleted:
			for _, mutation := range pendingDeletes[record.ID] {
				if err := o.store.FinishMutation(ctx, mutation.OperationID, storage.MutationSucceeded,
					record.ExternalID, "snapshot already deleted", o.now()); err != nil {
					return err
				}
			}
		case storage.SnapshotFailed:
			// Failed image builds are terminal and need no provider reconciliation.
		}
	}
	return nil
}

// acquireManagedImage returns the current image while atomically pinning it
// against stale-image deletion for an in-flight reset.
func (o *Orchestrator) acquireManagedImage() string {
	o.mu.Lock()
	id := o.managedImage
	if id != "" {
		o.imageRefs[id]++
	}
	o.mu.Unlock()
	return id
}

func (o *Orchestrator) releaseImage(id string) {
	if id == "" {
		return
	}
	o.mu.Lock()
	if o.imageRefs[id] <= 1 {
		delete(o.imageRefs, id)
	} else {
		o.imageRefs[id]--
	}
	o.mu.Unlock()
}

func (o *Orchestrator) imageInUse(id string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.imageRefs[id] > 0
}

func (o *Orchestrator) activateStoredSnapshot(ctx context.Context, image provider.ManagedImage) error {
	record, err := o.ensureStoredSnapshot(ctx, image)
	if err != nil || o.store == nil || record.ID == 0 {
		return err
	}
	return o.store.ActivateSnapshot(ctx, record.ID, image.ID, image.SizeBytes, o.now())
}

func (o *Orchestrator) beginStoredSnapshot(ctx context.Context, sourceResourceID int64, fingerprint string) (storage.Snapshot, error) {
	if o.store == nil {
		return storage.Snapshot{}, nil
	}
	name := o.cfg.Tag + "-golden-" + fingerprint[:12]
	snapshot, err := o.store.BeginSnapshot(ctx, storage.Snapshot{
		OperationID: "create-snapshot-" + shortID(), Provider: o.cfg.ProviderName,
		Driver: o.cfg.Driver, Tier: o.cfg.Tier, Name: name, Fingerprint: fingerprint,
		SourceResourceID: sourceResourceID, State: storage.SnapshotBuilding, CreatedAt: o.now(),
	})
	o.storageError("begin snapshot", err)
	return snapshot, err
}

func (o *Orchestrator) failStoredSnapshot(ctx context.Context, snapshot storage.Snapshot, detail string) {
	if o.store == nil || snapshot.ID == 0 {
		return
	}
	if err := o.store.SetSnapshotState(ctx, snapshot.ID, storage.SnapshotFailed, detail, o.now()); err != nil {
		o.storageError("fail snapshot", err)
	}
}

func (o *Orchestrator) ensureStoredSnapshot(ctx context.Context, image provider.ManagedImage) (storage.Snapshot, error) {
	if o.store == nil {
		return storage.Snapshot{}, nil
	}
	records, err := o.store.Snapshots(ctx, storage.SnapshotFilter{Provider: o.cfg.ProviderName, Tier: o.cfg.Tier})
	if err != nil {
		return storage.Snapshot{}, err
	}
	for _, record := range records {
		if record.ExternalID == image.ID {
			return record, nil
		}
		if record.ExternalID == "" && record.Name == image.Name && record.Fingerprint == image.Fingerprint {
			if err := o.store.ActivateSnapshot(ctx, record.ID, image.ID, image.SizeBytes, o.now()); err != nil {
				return storage.Snapshot{}, err
			}
			record.ExternalID = image.ID
			record.SizeBytes = image.SizeBytes
			record.State = storage.SnapshotActive
			return record, nil
		}
	}
	record, err := o.store.BeginSnapshot(ctx, storage.Snapshot{
		OperationID: "adopt-snapshot-" + image.ID + "-" + shortID(), Provider: o.cfg.ProviderName,
		Driver: o.cfg.Driver, Tier: o.cfg.Tier, Name: image.Name, Fingerprint: image.Fingerprint,
		State: storage.SnapshotBuilding, CreatedAt: image.CreatedAt,
	})
	if err != nil {
		return storage.Snapshot{}, err
	}
	if err := o.store.ActivateSnapshot(ctx, record.ID, image.ID, image.SizeBytes, o.now()); err != nil {
		return storage.Snapshot{}, err
	}
	record.ExternalID = image.ID
	record.SizeBytes = image.SizeBytes
	record.State = storage.SnapshotActive
	return record, nil
}

func (o *Orchestrator) recordSnapshotStorageCost(ctx context.Context, snapshot storage.Snapshot, end time.Time) {
	start := snapshot.CompletedAt
	if start.IsZero() {
		start = snapshot.CreatedAt
	}
	if o.store == nil || snapshot.ID == 0 || start.IsZero() || end.Before(start) {
		return
	}
	entry := storage.CostEntry{
		SnapshotID: snapshot.ID, Kind: storage.CostSnapshotStorage, Estimated: true,
		StartedAt: start, EndedAt: end, RecordedAt: o.now(),
	}
	quote, err := o.store.LatestPriceQuote(ctx, o.cfg.ProviderName, o.cfg.InstanceType)
	if err == nil {
		entry.PriceQuoteID = quote.ID
		entry.Currency = quote.Currency
		entry.Known = snapshot.SizeBytes > 0
		if entry.Known {
			const bytesPerGB = int64(1_000_000_000)
			const month = 30 * 24 * time.Hour
			gbNanos := proportionalNanos(quote.SnapshotGBMonthNanos, time.Duration(snapshot.SizeBytes), time.Duration(bytesPerGB))
			entry.Nanos = proportionalNanos(gbNanos, end.Sub(start), month)
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		o.storageError("read snapshot price quote", err)
		return
	}
	if _, err := o.store.RecordCost(ctx, entry); err != nil {
		o.storageError("record snapshot storage cost", err)
	}
}
