# Sales Forecasting Analysis Notebooks

This directory contains Jupyter notebooks for analyzing and visualizing Snowflake ML forecasting results.

## Available Notebooks

### 1. `sales_forecast_analysis.ipynb`

**Purpose**: Complete analysis and visualization of sales forecasting results

**Contents**:
- ✓ Connect to Snowflake and load forecast data
- ✓ Summary statistics (actuals vs forecast)
- ✓ Time-series visualization with confidence intervals
- ✓ Zoomed view of recent actuals + forecast
- ✓ Monthly revenue breakdowns
- ✓ Forecast uncertainty analysis
- ✓ Historical seasonality patterns
- ✓ Day-of-week analysis
- ✓ Summary metrics table
- ✓ Export forecast to CSV

**Prerequisites**:
- Snowflake account with sales forecast data loaded
- Python with pandas, matplotlib, seaborn
- Valid `config/credentials.yaml`

**How to Run**:
```bash
# Start Jupyter
jupyter notebook

# Open sales_forecast_analysis.ipynb
# Execute cells from top to bottom
```

## Getting Started

1. **Run the forecast pipeline first**:
   ```bash
   python src/python/scripts/run_sales_forecast.py
   ```

2. **Start Jupyter**:
   ```bash
   jupyter notebook
   ```

3. **Open and run notebook**:
   - Navigate to `notebooks/sales_forecast_analysis.ipynb`
   - Click "Cell" → "Run All" or execute cells individually

## Key Visualizations

### 1. Full Time Series
- Historical actuals (blue)
- 90-day forecast (orange)
- 95% confidence interval (shaded)

### 2. Recent Period Zoom
- Last 60 days of actuals
- Full 90-day forecast
- Clear transition point

### 3. Monthly Breakdowns
- Total forecasted revenue by month
- Average daily forecast trends

### 4. Uncertainty Analysis
- Confidence interval width over time
- Relative uncertainty as % of forecast

### 5. Seasonality Patterns
- Historical monthly seasonality
- Day-of-week patterns (weekday vs weekend)

## Common Tasks

### View Forecast Data
```python
# Display first 20 forecast records
df_pred.head(20)

# Get forecast statistics
df_pred['forecast_revenue'].describe()
```

### Modify Visualizations
```python
# Change chart colors
ax.plot(..., color='#2ca02c')  # Green

# Adjust figure size
plt.rcParams['figure.figsize'] = (16, 8)

# Add custom title
ax.set_title('My Custom Title', fontsize=14, fontweight='bold')
```

### Export Results
```python
# Export forecast to CSV
df_pred.to_csv('forecast_results.csv', index=False)

# Export with custom columns
export_data = df_pred[['date', 'forecast_revenue', 'lower_bound']].copy()
export_data.to_csv('forecast_summary.csv', index=False)
```

## Troubleshooting

### "Connection refused"
- Verify Snowflake account and credentials in `config/credentials.yaml`
- Check warehouse is running
- Ensure VPN is connected if required

### "View not found"
- Verify forecast pipeline has been executed successfully
- Check schema name matches configuration
- Confirm objects exist: `SHOW VIEWS LIKE '%FORECAST%';`

### "No data returned"
- Confirm forecast tables are populated
- Check date range in queries
- Verify you're connected to correct database

## Next Steps

1. **Create dashboards**: Connect results to Looker/Tableau
2. **Alert setup**: Implement alerts when actuals exceed bounds
3. **Model retraining**: Schedule monthly model updates
4. **Feature engineering**: Add external variables (marketing spend, holidays)
5. **Ensemble methods**: Combine multiple forecasts for better accuracy

## Resources

- [Jupyter Documentation](https://jupyter.org/documentation)
- [Matplotlib Visualization](https://matplotlib.org/)
- [Seaborn Styling](https://seaborn.pydata.org/)
- [Pandas Data Analysis](https://pandas.pydata.org/)

---

**Last Updated**: April 15, 2026
