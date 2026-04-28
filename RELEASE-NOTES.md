# Release Notes

These notes summarize the backend release history reconstructed from the git
history. New releases should add a section at the top when the application
version changes.

## ks-backend v. 1.3.0

- **Guest Play**: added session-backed guest account creation via
  `/api/auth/guest`, using a 200-by-200 chess-player-inspired name pool and
  collision-safe `guest_first_last` usernames. Guest sessions use a one-year
  sliding lifetime so the same browser keeps its guest identity much longer.

## ks-backend v. 1.2.38

- **Clock Start**: newly active games now keep both clocks paused until White
  completes the first legal move; failed opening attempts do not start either
  player's clock.

## ks-backend v. 1.2.37

- **Material Summary API**: game state responses now expose engine-derived
  remaining material, including public pawn-capture counts for Cincinnati and
  Wild 16.

## ks-backend v. 1.2.36

- **Wild 16 Review Accuracy**: completed review transcripts now normalize
  legacy pawn-try counts, so promotion captures count as one pawn try instead
  of one per promotion piece.

## ks-backend v. 1.2.35

- **Engine Update**: bumped the `kriegspiel` engine dependency for the Wild 16
  pawn-try count fix.

## ks-backend v. 1.2.34

- **Wild 16 Reviews**: completed reviews now show private illegal attempts in
  the referee view while preserving live-game privacy.

## ks-backend v. 1.2.33

- **Wild 16 Attempts**: updated the engine dependency so failed Wild 16 attempts
  are removed from the current askable move list.
- **CI Alignment**: updated test dependencies to use the same engine behavior.

## ks-backend v. 1.2.32

- **Current Message Ordering**: turn-start announcements now appear before move
  results in game and review message summaries.

## ks-backend v. 1.2.31

- **Turn Announcements**: pawn-capture availability is attached to the next
  player's turn instead of the previous player's completed move.

## ks-backend v. 1.2.30

- **Monitoring**: backend restarts now emit a Sentry event so deployments and
  machine reboots are visible in production telemetry.

## ks-backend v. 1.2.29

- **Crash Reporting**: added Sentry integration for backend exceptions with
  release metadata.

## ks-backend v. 1.2.28

- **Ruleset UI Safety**: Berkeley "Any?" actions are hidden outside
  Berkeley+Any games.

## ks-backend v. 1.2.27

- **Ruleset Isolation**: backend actions are now validated against the active
  ruleset to prevent Berkeley, Cincinnati, and Wild 16 behavior from leaking
  into each other.

## ks-backend v. 1.2.26

- **Engine State Compatibility**: added support for canonical engine-state
  schema 4 payloads.

## ks-backend v. 1.2.25

- **New Rulesets**: added backend support for Cincinnati and Wild 16 games in
  game creation, state polling, histories, and review transcripts.

## ks-backend v. 1.2.24

- **Canonical Engine Migration**: added schema 4 migration support for stored
  engine state and optimized schema 3 upgrades.
- **Engine Packaging**: switched backend deployment to consume the PyPI
  `kriegspiel` package where appropriate.

## ks-backend v. 1.2.23

- **Replay Orientation**: replay transcripts now include viewer color so the
  frontend can orient the board for the reviewing user.

## ks-backend v. 1.2.22

- **Canonical Serialization**: added migration tooling for canonical engine
  state serialization.
- **Legacy Safety**: migration paths can synthesize scoresheets when older
  transcripts drift from stored engine state.

## ks-backend v. 1.2.21

- **History Accuracy**: user game history now reports full turns correctly.

## ks-backend v. 1.2.20

- **Regression Fixes**: tightened backend lifecycle, auth, and resignation
  behavior found by remote regression tests.
- **Persistence Safety**: sync terminal-game persistence is limited to human
  games.

## ks-backend v. 1.2.19

- **Completed Games**: completed-game codes are preserved consistently.

## ks-backend v. 1.2.18

- **Lobby Cleanup**: waiting games now expire automatically after ten minutes.

## ks-backend v. 1.2.17

- **Password Handling**: long passwords are supported safely.
- **Review Cleanup**: impossible attempts are omitted from review history.

## ks-backend v. 1.2.16

- **Account Validation**: relaxed username and password validation rules.

## ks-backend v. 1.2.15

- **Bot Reports**: listed-bot reports now include daily result breakdowns.

## ks-backend v. 1.2.14

- **Bot Reports**: added the listed-bots daily report endpoint and fixed async
  iteration in the report path.

## ks-backend v. 1.2.13

- **Legacy Cleanup**: removed legacy auth and rating fallback paths.

## ks-backend v. 1.2.12

- **Bot Metadata**: added tooling to impute missing bot owner emails.

## ks-backend v. 1.2.11

- **Scoresheet Storage**: removed legacy root scoresheet compatibility.

## ks-backend v. 1.2.10

- **Ratings Tools**: added an archive rating recalculation script.

## ks-backend v. 1.2.9

- **Ratings Backfill**: realigned overall totals during stats backfill.

## ks-backend v. 1.2.8

- **Ratings Backfill**: recomputed unsynced track result statistics.

## ks-backend v. 1.2.7

- **Ratings API**: added backend-owned rating summaries and rating history.

## ks-backend v. 1.2.6

- **Elo History**: fixed track-specific Elo history snapshots.

## ks-backend v. 1.2.5

- **Bot Profiles**: exposed bot owner email on public bot profiles.

## ks-backend v. 1.2.4

- **Game History**: player game history now includes the game ruleset.

## ks-backend v. 1.2.3

- **Game History**: fixed history game codes and completed result reasons.

## ks-backend v. 1.2.2

- **Routing**: game routes now use stable game codes.

## ks-backend v. 1.2.1

- **Review Metadata**: extended review-game metadata for richer replay pages.

## ks-backend v. 1.2.0

- **Ratings**: added multi-track Elo ratings.
- **History Metadata**: added opponent role information to game history.

## ks-backend v. 1.1.12

- **Timeout Sweep**: set the stale-game timeout sweep interval to 25 minutes.

## ks-backend v. 1.1.11

- **Timeouts**: stale active-game timeouts are swept in the background.

## ks-backend v. 1.1.10

- **Performance**: active games are cached in memory.

## ks-backend v. 1.1.9

- **Bot History**: fixed public bot game history.

## ks-backend v. 1.1.8

- **Bot Auth**: sped up bot token authentication.

## ks-backend v. 1.1.7

- **Scoresheets**: removed duplicate root scoresheet storage.

## ks-backend v. 1.1.6

- **Lobby Stats**: added a lobby game stats endpoint.

## ks-backend v. 1.1.5

- **Game Metadata**: game metadata now uses live player Elo.

## ks-backend v. 1.1.4

- **Bot Rulesets**: added bot ruleset support metadata.

## ks-backend v. 1.1.3

- **Bot Games**: waiting bot games are persisted and public bot profiles are
  exposed.

## ks-backend v. 1.1.2

- **Move Repair**: repaired missing forced pawn-capture states.

## ks-backend v. 1.1.1

- **Move Repair**: repaired empty possible-to-ask states.

## ks-backend v. 1.1.0

- **Bot Lobby**: limited bot lobby game creation and joins.
- **Ratings Metadata**: exposed bot and opponent ratings.

## ks-backend v. 1.0.0

- **Production Backend**: promoted the backend to the first stable release with
  FastAPI app factory, MongoDB health/readiness, auth/session routes, game
  lifecycle APIs, engine adapter, clocks, transcripts, recent history, profile
  history, leaderboard APIs, review APIs, structured logging, and baseline
  deployment contracts.
- **Bot Support**: added bot registration, bot games, bot authentication, and
  bot-facing allowed-move APIs.
- **Game Records**: added persistent per-player scoresheets, structured
  referee announcements, backend version health reporting, and Elo ratings.

## ks-backend v. 0.1.0

- **Initial Scaffold**: created the backend package, early tests, database
  tables, formatting hooks, user model, runtime entrypoint, and basic
  development stack.
