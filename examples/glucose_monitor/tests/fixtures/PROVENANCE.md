# Test fixtures — provenance

**Synthetic.** `graph.json` is *not* a capture. LibreLinkUp serves only a signed-in follower's own
patients, so a real capture would be someone's health data and name. The fixture is hand-built
instead, to the response shape the community clients document — the `GraphResponse`,
`Connection` and `GlucoseItem` interfaces of
[`nightscout-librelink-up`](https://github.com/timoschlueter/nightscout-librelink-up) (MIT), read
2026-10-29 — with an invented patient (`Erika Mustermann`), invented ids and a placeholder ticket.

| file | stands for | shape |
|---|---|---|
| `graph.json` | `GET https://api-de.libreview.io/llu/connections/{patientId}/graph` | the envelope `{status, data, ticket}`; `data` holds the `connection` (with its latest `glucoseMeasurement`), `activeSensors` and ten `graphData` points |

What it is built to exercise:

- **Timestamps in LibreLinkUp's US style without zero padding** (`10/29/2026 4:45:07 AM`), with
  `FactoryTimestamp` in UTC and `Timestamp` an hour ahead (CET, the phone's clock), so the
  derived UTC offset is +60 minutes.
- **The two series**: the `live` measurement carries `TrendArrow`; the `history` points do not,
  and sit on a 15-minute grid behind it.
- **A credential in the envelope**: `ticket.token` is the renewed bearer token every response
  carries, the part the extractor must never back up.

## Verifying against the real API

Before trusting the field names further, compare against a real response of your own:
sign in with `uv run poe request-glucose account me`, run `run-glucose-ingest`, and read a record
of `glucose-raw` in Kafbat (<http://localhost:8080>). Do not commit a real capture.
