# snow-feeds

SnowBrains' feed collector. Every 20 minutes, around the clock, GitHub runs:

1. **feedfactory.py**: road closures, avalanche reports and warnings, page watch, news searches and the Google News relay.
2. **collect.py**: reads every feed in `Subscriptions.opml` (as NetNewsWire does) into `nnw.sqlite`.

The results go to the `data` branch. The SnowBrains hourly story check reads them from there, so it no longer needs the Mac to be on.

When you add or remove feeds in NetNewsWire, export the subscription list (File > Export Subscriptions) and upload it here as `Subscriptions.opml`, replacing the old one.
