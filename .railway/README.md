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

Two things must be settled before anyone applies:

- **This file describes only `pubky-marketplace-staging`
  (`c991d768-4a3c-42ea-b5ed-eaa22d4916ed`).** The same service also runs in
  `pubky-marketplace-production` (`75faa4fe-466c-4277-977f-1d8e4e31df8c`), which
  this file does not cover. Applying it will not configure production, and
  production would keep drifting on its own dashboard settings.
- **The `preserve()` entries are load-bearing.** Config-as-code treats an
  omitted variable as a deletion. Dropping any of them plans a real delete of
  that variable on the live service, including the encryption keys.

## Notes

- `railway config plan` is safe and does not change Railway.
- `railway config apply` previews changes and asks before applying unless you pass `--yes`.
- Destructive changes in non-interactive or agent sessions require `railway config apply --confirm-destructive` after reviewing the plan.
- CI should pin a plan (`railway config plan --out railway-plan.json`) and apply that file on merge (`railway config apply --plan railway-plan.json --yes --confirm-destructive`) so the reviewed change set is what lands. On GitHub Actions, use https://github.com/railwayapp/config.
- Services already managed by `railway.json` must be migrated before `.railway/railway.ts` can manage them.
- Keep one `.railway` file for the whole project. A named `export const partial` (or `PARTIAL` / `const Partial`) is a last resort for separate repos that cannot share that file. Do not add it unless omit=delete across repos is a blocker.
- Use `replicas` for scaling; advanced placement can still specify region names.
- Use `group("Name", [resources])` to keep large projects organized on the Railway canvas.
- Secrets imported from Railway are rendered as `preserve()` so existing values are retained without writing secret values to source. Use `railway config pull --omit-preserved-variables` for a smaller import. `railway config pull --include-variables` decrypts and inlines non-sealed values (including secrets that were never sealed).
- `railway config migrate` finds every `railway.json` / `railway.toml` in the repository and writes them into this one file.
