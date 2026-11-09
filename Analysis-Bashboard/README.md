# Analysis dashboard (`analysis_bashboard` package)

**Status: prototype.** `gtc-dashboard` starts a Dash app at
http://127.0.0.1:8050 that shows the planned layout as coloured placeholder
boxes. No data or charts are wired in yet. Goals and audience are in
`dashboard-brief.md` (half filled in).

| File | Purpose |
|---|---|
| `src/app.py` | Dash app and layout (`gtc-dashboard`) |
| `src/assets/style.css` | Dark theme, auto-loaded by Dash |
| `src/data.py` | `load_lineitem_analysis()`: reads the `lineitem_analysis` table |
| `src/figure.py` | Ready-made Plotly charts: running total over time, top-N horizontal/vertical bars, pie |
| `src/sql/lineitem_analysis_all.sql` | Query used by `data.py` |
| `notebooks/money_in_out.py` | Scratch script: sales vs AmEx expenses over time (opens charts in a browser) |

Try a chart without the app:

```python
from analysis_bashboard.data import load_lineitem_analysis
from analysis_bashboard import figure
df = load_lineitem_analysis()
figure.create_hbar_chart(df, "Top organizations by sales", "organization_name", "money_value", 10).show()
```
