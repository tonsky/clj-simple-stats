# 1.4.0 - Jul 22, 2026

- Cache DB connection between dashboard accesses
- More compact `rollup_daily` keeping top 20 per day instead of `daily_counts`
- Drop `rollup_state`

# 1.3.0 - Jul 20, 2026

- Per-day rollup table `daily_counts`
- Unique visitor counts are now per-day instead of deduplicated across the whole range

# 1.2.0 - Jan 31, 2026

- Guard against SQL injection
- Remove stats cookie, IP and user-agent from DB

# 1.1.0 - Dec 17, 2025

- Use Inter font in Dashboard
- Hover over any place in the graph
- Click to filter by 1 day

# 1.0.0 - Dec 12, 2025

- Initial
