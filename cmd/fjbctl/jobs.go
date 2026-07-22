package main

import (
	"flag"
	"io"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	controlv1 "github.com/hstern/fj-bellows/gen/fjbellows/control/v1"
)

func cmdJobs(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cf commonFlags
	registerCommonFlags(fs, &cf)
	since := fs.String("since", "", "only jobs first seen within this duration (for example 24h)")
	from := fs.String("from", "", "inclusive RFC3339 first-seen time")
	to := fs.String("to", "", "exclusive RFC3339 first-seen time")
	tier := fs.String("tier", "", "only jobs assigned to this tier")
	provider := fs.String("provider", "", "only jobs assigned to this named provider")
	repository := fs.String("repository", "", "only this repository")
	workflow := fs.String("workflow", "", "only this workflow identity")
	status := fs.String("status", "", "only this durable job status")
	limit := fs.Int("limit", 100, "maximum rows (1-1000)")
	cursor := fs.String("cursor", "", "opaque cursor from a previous response")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *limit < 1 || *limit > 1000 {
		outln(stderr, "fjbctl: -limit must be between 1 and 1000")
		return 2
	}
	start, end, err := parseReportWindow(*since, *from, *to)
	if err != nil {
		return fmtErr(stderr, err)
	}
	client, err := cf.client()
	if err != nil {
		return fmtErr(stderr, err)
	}
	ctx, cancel := contextWithTimeout()
	defer cancel()
	resp, err := client.JobHistory(ctx, connect.NewRequest(&controlv1.JobHistoryRequest{
		From: start, To: end, Tier: *tier, Provider: *provider, Repository: *repository,
		Workflow: *workflow, Status: *status, Limit: int32(*limit), Cursor: *cursor,
	}))
	if err != nil {
		return fmtErr(stderr, err)
	}
	if cf.json {
		return printJSON(stdout, stderr, resp.Msg)
	}
	renderJobs(stdout, resp.Msg.Jobs)
	if resp.Msg.NextCursor != "" {
		outf(stdout, "next_cursor: %s\n", resp.Msg.NextCursor)
	}
	return 0
}

func renderJobs(w io.Writer, jobs []*controlv1.Job) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	outln(tw, "TIME\tTIER\tPROVIDER\tREPOSITORY\tWORKFLOW\tJOB\tSTATUS\tQUEUE\tRUN")
	for _, job := range jobs {
		at := "-"
		if job.FirstSeenAt != nil {
			at = job.FirstSeenAt.AsTime().Local().Format(time.RFC3339)
		}
		workflow := job.WorkflowFile
		if workflow == "" {
			workflow = job.Workflow
		}
		if workflow == "" {
			workflow = job.JobName
		}
		outf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			at, emptyDash(job.Tier), emptyDash(job.Provider), emptyDash(job.Repository),
			emptyDash(workflow), emptyDash(job.JobName), emptyDash(job.Status),
			protoInterval(job.QueuedAt, job.DispatchedAt),
			protoInterval(job.RunnerStartedAt, job.RunnerFinishedAt))
	}
	_ = tw.Flush()
	if len(jobs) == 0 {
		outln(w, "(no jobs)")
	}
}
