# Railway configuration

This project defines its Railway infrastructure in code.

```txt
.railway/railway.ts
```

Use this file to describe the Railway project you want: services, databases, buckets, custom domains, replicas, groups, and environment variables.

The TypeScript file imports `railway/iac`. Install the SDK from the repository root:

```bash
npm install railway
```

## Common commands

Create the configuration files:

```bash
railway config init
```

Import an existing Railway project into code:

```bash
railway config pull
```

Preview what Railway would change:

```bash
railway config plan
```

Apply the planned changes:

```bash
railway config apply
```

## Status in this repo

`railway.toml` is still present and is still what Railway reads on a git deploy.
Nothing here is live yet. Deleting `railway.toml` before running
`railway config apply` would leave the service with no build or healthcheck
configuration.

`.railway/railway.ts` covers both Railway projects this service runs in:

- `pubky-marketplace-staging` (`c991d768-4a3c-42ea-b5ed-eaa22d4916ed`)
- `pubky-marketplace-production` (`75faa4fe-466c-4277-977f-1d8e4e31df8c`)

The format is one authoring file per repository, evaluated against the
**linked** project. Railway does not let one `defineRailway` return two
projects at once. Branch on `ctx.projectId` (not `ctx.environment`): both
projects name their Railway environment `production`. Docs:
[Infrastructure as Code reference — Environment context](https://docs.railway.com/infrastructure-as-code/reference)
and [railway config](https://docs.railway.com/cli/config) (`plan`/`apply` use
the directory link; `--file` only overrides the authoring path).

`export const partial = "marketplace-service"` remains required. Each Railway
project also has other services this repo must not omit=delete.

**The `preserve()` entries are load-bearing.** Config-as-code treats an
omitted variable as a deletion. Dropping any of them plans a real delete of
that variable on the live service, including the encryption keys. Staging and
production do not share an identical variable set; the file lists each
project's names separately.

## Applying (operator)

Do this once per project, after `railway config plan` shows **no variable
deletes** and only the intended build/deploy updates. Never apply from an
unreviewed plan.

1. **Staging first.** From this repo (already linked to staging in typical
   checkouts): `railway status` must show `pubky-marketplace-staging`. Run
   `railway config plan`, then `railway config apply` only if the plan is
   limited to `marketplace-service` build/deploy fields.
2. **Production second.** Link a scratch directory (do not flip this worktree
   back and forth): `mkdir -p /tmp/marketplace-service-prod && cd` there,
   `railway link -p 75faa4fe-466c-4277-977f-1d8e4e31df8c -e production -s marketplace-service`,
   then `railway config plan --file /path/to/this/repo/.railway/railway.ts`.
   Apply from that same scratch directory with the same `--file` only if the
   plan has zero deletes.
3. **Restore** this worktree's link to staging if you had to change it:
   `railway link -p c991d768-4a3c-42ea-b5ed-eaa22d4916ed -e production -s marketplace-service`.
4. **Do not delete `railway.toml` until both projects have been applied** and
   a later git deploy no longer needs the toml for builder/healthcheck. After
   both applies succeed, a follow-up can remove the toml and confirm Settings
   no longer point at a Config as Code path.

## Notes

- `railway config plan` is safe and does not change Railway.
- `railway config apply` previews changes and asks before applying unless you pass `--yes`.
- Destructive changes in non-interactive or agent sessions require `railway config apply --confirm-destructive` after reviewing the plan.
- CI should pin a plan (`railway config plan --out railway-plan.json`) and apply that file on merge (`railway config apply --plan railway-plan.json --yes --confirm-destructive`) so the reviewed change set is what lands. On GitHub Actions, use https://github.com/railwayapp/config. A project token is scoped to one environment, so staging and production need separate tokens/workflows.
- Services already managed by `railway.json` must be migrated before `.railway/railway.ts` can manage them.
- Keep one `.railway` file for the whole project. A named `export const partial` (or `PARTIAL` / `const Partial`) is a last resort for separate repos that cannot share that file. Do not add it unless omit=delete across repos is a blocker.
- Use `replicas` for scaling; advanced placement can still specify region names.
- Use `group("Name", [resources])` to keep large projects organized on the Railway canvas.
- Secrets imported from Railway are rendered as `preserve()` so existing values are retained without writing secret values to source. Use `railway config pull --omit-preserved-variables` for a smaller import. `railway config pull --include-variables` decrypts and inlines non-sealed values (including secrets that were never sealed).
- `railway config migrate` finds every `railway.json` / `railway.toml` in the repository and writes them into this one file.
