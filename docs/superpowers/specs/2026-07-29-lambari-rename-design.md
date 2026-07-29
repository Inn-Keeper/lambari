# Lambari Rename Design

## Goal

Rename the project from Tripwire to Lambari across its source, configuration,
documentation, repository, and local checkout, leaving no active Tripwire
identity behind.

## Rename contract

- Use `Lambari` for user-visible product names.
- Use `lambari` for machine-readable names, Go module imports, package names,
  repository paths, log text, and the Kafka consumer group.
- Replace `TRIPWIRE_ADDR` and `TRIPWIRE_KAFKA_BROKERS` with `LAMBARI_ADDR` and
  `LAMBARI_KAFKA_BROKERS`.
- Rename the GitHub repository from `Inn-Keeper/tripwire` to
  `Inn-Keeper/lambari`.
- Rename the local checkout directory from `tripwire` to `lambari`.
- Do not retain compatibility aliases because the project is an early proof of
  concept and the requested rename is complete.

## Implementation

Update all tracked text references, including Go imports, pnpm workspace names
and filters, application copy, HTML metadata, documentation, diagrams, comments,
environment-variable documentation, Kafka configuration, and source links.
Refresh generated dependency metadata only if the package-name changes require
it.

After the in-repository rename passes verification, commit and push it, rename
the GitHub repository, update the origin URL if Git does not do so automatically,
and finally rename the local checkout directory.

## Verification

- Search tracked and relevant untracked files case-insensitively for `tripwire`;
  expect no product-identity references.
- Run the repository's backend tests and frontend production build.
- Confirm the frontend package filter and Go module imports resolve under the
  Lambari names.
- Confirm the GitHub repository and `origin` use `Inn-Keeper/lambari`.
- Confirm the local checkout directory is named `lambari`.
- Confirm the final branch is pushed and the worktree is clean.

## Rollback

Git history preserves the source changes. GitHub repository renames retain a
redirect from the former URL, and the local directory can be renamed back if
needed.
