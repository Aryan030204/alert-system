module.exports = {
  allowedMetrics: [
    "aov",
    "total_sales",
    "total_orders",
    "total_sessions",
    "total_atc_sessions",
    "conversion_rate"
  ],

  metricDescriptions: {
    aov: "Average Order Value",
    total_sales: "Total Sales During Interval",
    total_orders: "Number of Orders",
    total_sessions: "Total Website Sessions (from Shopify)",
    total_atc_sessions: "Add to Cart Sessions (from Shopify)",
    conversion_rate: "Conversion Rate (Orders/Sessions %)"
  },

  // Data source mapping for documentation
  metricSources: {
    // From hourly_sessions_summary_shopify (Shopify data)
    total_sessions: "hourly_sessions_summary_shopify.number_of_sessions",
    total_atc_sessions: "hourly_sessions_summary_shopify.number_of_atc_sessions",
    
    // From hour_wise_sales (legacy/orders data)
    total_sales: "hour_wise_sales.total_sales",
    total_orders: "hour_wise_sales.number_of_orders",
    
    // Derived metrics (cross-table)
    aov: "hour_wise_sales.total_sales / hour_wise_sales.number_of_orders",
    conversion_rate: "hour_wise_sales.number_of_orders / hourly_sessions_summary_shopify.number_of_sessions"
  }
};
