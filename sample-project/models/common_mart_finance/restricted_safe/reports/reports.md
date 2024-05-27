{% docs atr_change_flag %}

Field showing movement of the top subscriptions by arr each month. Available values - 'Became available to renew', 'Dropped as available to renew', 'New subscription', 'Subscription ends this month' or 'Top available to renew excluded deal'. These are calculated as shown below:

### Top available to renew excluded deal

This flags subscriptions where the `is_available_to_renew` flag is false. The top 10 by arr are shown each month.

### Subscription ends this month

Either the multi_year_booking_subscription_end_month or bookings_term_end_month is in the month being checked.

### Became available to renew

This model uses the daily snapshot of mart_available_to_renew so by using the LAG function to compare the `is_available_to_renew` flag to the previous day, we can see which went from false to true indicating that they became available to renew in that month. The top 10 by arr are shown.

### Dropped as available to renew

The inverse of the above. The `is_available_to_renew` flag went from true to false.

### New subscription

A new `subscription_name` has appeared this month, limited to top 10 by arr.

{% enddocs %}
