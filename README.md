# Simple statistics for Clojure/Ring web apps

This library was born out of a desire to have a basic understanding of my web projects’ visitors and a lack of desire to set up anything.

`clj-simple-stats`, as the name suggests, is trivial to set up:

1. Add `io.github.tonsky/clj-simple-stats {:mvn/version "1.0.0"}` to `deps.edn`.
2. Add `clj-simple-stats.core/wrap-stats` anywhere in your middleware stack.

That’s it! There’s no step 3.

- All the visits that go through that middleware will be counted automatically.
- Data will be stored to `clj_simple_stats.duckdb` in the current dir.
- You’ll get a dashboard at `/stats` that looks like this:

![](./docs/dashboard.webp)

Couple of highlights:

- Statistic is fully server-side, no JS is used.
- You can narrow down stats per URL, query, referrer, user agent, etc. (click on the looking glass icon).
- We try our best to distinguish between human visits, RSS readers, and scrapers.
- The goal is to understand how many people see your site, not how many requests.
- Only responses with status 200 and content-type: `text/html`, `application/atom+xml` or `application/rss+xml` are counted
- Tracking cookie is used only to uniquely identify visitors; no cookie banner is needed.
- Only two external dependencies: DuckDB and Ring (which you probably have anyway).

## Configuration

These default values are used; feel free to override:

```clojure
(wrap-stats handler
  {:db-path       "clj_simple_stats.duckdb"
   :uri           "/stats"
   :dash-perms-fn (fn [req] true)
   :cookie-name   "stats_id"
   :cookie-opts   {:max-age   2147483647
                   :path      "/"
                   :http-only true}})
```

`wrap-stats` is a composition of `wrap-collect-stats` and `wrap-render-stats`, which you can use separately as well.

Finally, `wrap-render-stats` middleware checks if `(= (:uri opts) (:uri req))` and then calls `render-stats` handler. If you prefer to use your own router, feel free to use `render-stats` handler directly.

## In the wild

You can see live deployments at

- [grumpy.website/stats](https://grumpy.website/stats)
- [tonsky.me/stats](https://tonsky.me/stats)

## License

Copyright © 2025 Nikita Prokopov

Licensed under [MIT](LICENSE).
