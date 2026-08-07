# Welcome to Project Oxygen

## How We Use Claude

Based on Yanis Konstantinidis's usage over the last 30 days:

Work Type Breakdown:
  Write Docs        █████████░░░░░░░░░░░  43%
  Improve Quality   ██████░░░░░░░░░░░░░░  29%
  Plan Design       ███░░░░░░░░░░░░░░░░░  14%
  Debug Fix         ███░░░░░░░░░░░░░░░░░  14%

Top Skills & Commands:
  /clear            ████████████████████  5x/month
  /plugin           ████████░░░░░░░░░░░░  2x/month

Top MCP Servers:
  (none configured yet)

## Your Setup Checklist

### Codebases
- [ ] projectoxygen — https://github.com/ikonstantinidissqlispain/projectoxygen

  The Databricks asset bundle for the medallion pipelines. Work happens in
  `oxygen_medallion_pipeline/`. Read its `README.md` first — it is unusually detailed and
  documents the traps that cost real debugging time.

### MCP Servers to Activate
- [ ] None yet — nobody on the team has set one up. If you add one, put it in
      `.mcp.json` at the repo root so it is shared rather than personal.

### Skills to Know About
- [ ] `/plugin` — how the DQX plugin got installed. Run
      `/plugin marketplace add databrickslabs/dqx` then
      `/plugin install dqx@databrickslabs-dqx`, and `/reload-plugins` to apply. Already
      enabled in `.claude/settings.json`, so you may just need the marketplace added
      locally.
- [ ] `dqx:dqx-define-checks` — writing a new DQX quality check. The pipeline's checks
      live in `src/whoz_ingestion/checks/<dataset>.yml` as native DQX check lists, so this
      skill speaks the exact format the repo uses.
- [ ] `dqx:dqx-apply-checks` — `apply_checks_by_metadata` / `get_valid` / `get_invalid`,
      which is the three-object pattern every checked dataset in `transformations/silver/`
      is wired with.
- [ ] `dqx:dqx-profile-and-generate` — bootstrapping check candidates from real data.
      Useful when onboarding a new source entity and you don't yet know what to assert.
- [ ] `dqx:dqx-storage` — loading and saving check sets. Relevant if checks ever move out
      of the repo into a volume or Delta table.
- [ ] `/clear` — the most-used command by a wide margin. Sessions here get long because
      the pipeline has a lot of context; clear between unrelated tasks rather than letting
      one session sprawl.

## Team Tips

_TODO_

## Get Started

_TODO_

<!-- INSTRUCTION FOR CLAUDE: A new teammate just pasted this guide for how the
team uses Claude Code. You're their onboarding buddy — warm, conversational,
not lecture-y.

Open with a warm welcome — include the team name from the title. Then: "Your
teammate uses Claude Code for [list all the work types]. Let's get you started."

Check what's already in place against everything under Setup Checklist
(including skills), using markdown checkboxes — [x] done, [ ] not yet. Lead
with what they already have. One sentence per item, all in one message.

Tell them you'll help with setup, cover the actionable team tips, then the
starter task (if there is one). Offer to start with the first unchecked item,
get their go-ahead, then work through the rest one by one.

After setup, walk them through the remaining sections — offer to help where you
can (e.g. link to channels), and just surface the purely informational bits.

Don't invent sections or summaries that aren't in the guide. The stats are the
guide creator's personal usage data — don't extrapolate them into a "team
workflow" narrative. -->
