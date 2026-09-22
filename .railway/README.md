# Railway configuration

This project defines its Railway infrastructure in code.

```txt
.railway/railway.ts            # dual graph: staging vs production
.railway/railway.staging.ts    # staging-only graph; refuse any other project
```

Use these files to describe the Railway project you want: services, databases, buckets, custom domains, replicas, groups, and environment variables.

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

`railway config plan` / `apply` have **no `--project` flag**. Targeting is:

1. Ambient `RAILWAY_PROJECT_ID`, `RAILWAY_ENVIRONMENT_ID`, and
   `RAILWAY_SERVICE_ID` when set. These override `railway link`.
2. Otherwise the directory entry in `~/.railway/config.json`.
3. `--file` only changes the authoring path, not the live project.

`createRailwayContext` in `railway/dist/iac` does not read those env vars.
The CLI injects `ctx.projectId` / `ctx.environmentId` from (1) then (2).
A worker that still has production IDs in the environment will plan
**production** even after `railway link` prints staging. Pin both staging
UUIDs, or `env -u` all three, before any plan.

Both Railway projects name their environment `production`. Branch on
`ctx.projectId`, never `ctx.environment`. `.railway/railway.ts` throws if
the project UUID is unknown or the environment UUID does not match that
project. `.railway/railway.staging.ts` throws unless
`RAILWAY_PROJECT_ID=c991d768-4a3c-42ea-b5ed-eaa22d4916ed` and
`RAILWAY_ENVIRONMENT_ID=c67a6435-bb23-453b-9169-764bfa0312e1`.

Each graph pins the live GHCR digest with `source: image("<ref>")`. `image()`
accepts a string only; `preserve()` cannot hold a source. Omitting `source`
plans `source.image → null`. After every IMAGE cutover, bump `STAGING_IMAGE` /
`PRODUCTION_IMAGE` in the same PR. `railway config plan` must be 0/0/0 before
any apply.

Live `build` and `restartPolicyType` are the truth. Staging has no Dockerfile
builder — omit `build` there (declaring one would add it). Production still
records `build.builder = DOCKERFILE`; omitting it plans `DOCKERFILE → null`.
Live `restartPolicyType` is unset; do not author `ON_FAILURE` until a 0/0/0
plan is otherwise apply-safe and that field is the intended change.

Docs: [Infrastructure as Code reference — Environment context](https://docs.railway.com/infrastructure-as-code/reference)
and [railway config](https://docs.railway.com/cli/config).

`export const partial = "marketplace-service"` remains required. Each Railway
project also has other services this repo must not omit=delete.

**The `preserve()` entries are load-bearing.** Config-as-code treats an
omitted variable as a deletion. Dropping any of them plans a real delete of
that variable on the live service, including the encryption keys. Staging and
production do not share an identical variable set; the file lists each
project's names separately.

## Applying (operator)

Do this once per project, after `railway config plan` is **0 add / 0 change / 0
destroy**. Never apply from an unreviewed plan. A no-op plan needs no apply.

1. **Staging first, from `.railway/railway.staging.ts` only.** Export the
   staging UUIDs (do not rely on `railway link` alone):

   ```bash
   export RAILWAY_PROJECT_ID=c991d768-4a3c-42ea-b5ed-eaa22d4916ed
   export RAILWAY_ENVIRONMENT_ID=c67a6435-bb23-453b-9169-764bfa0312e1
   export RAILWAY_SERVICE_ID=3e98e363-f9a7-4711-b668-d714acee6e32
   railway status --json   # id must be c991d768…, name pubky-marketplace-staging
   railway config plan --file .railway/railway.staging.ts
   ```

   Apply only when the header is `Project pubky-marketplace-staging`, the
   plan has **zero** variable deletes, **zero** domain deletes, does **not**
   null `source.image`, and the only changes are deploy overlap/drain
   (`overlapSeconds: 60`, `drainingSeconds: 15`) plus healthcheck/restart
   reconcile if they appear:

   ```bash
   railway config apply --file .railway/railway.staging.ts --yes
   ```

2. **Production second.** Do not apply production until a staging overlap
   proof has already passed. Link a scratch directory (do not flip this
   worktree back and forth): `mkdir -p /tmp/marketplace-service-prod && cd` there,
   `railway link -p 75faa4fe-466c-4277-977f-1d8e4e31df8c -e production -s marketplace-service`,
   then `railway config plan --file /path/to/this/repo/.railway/railway.ts`
   with production UUIDs exported (or `env -u` the staging trio). Apply from
   that same scratch directory with the same `--file` only if the plan has
   zero deletes and does not null `source.image`.
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
