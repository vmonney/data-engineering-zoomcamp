{% macro get_vendors_names(vendor_id) %}
case 
    when {{ vendor_id }} = 1 then 'Creative Mobile Technologies'
    when {{ vendor_id }} = 2 then 'VeriFone Inc.'
    when {{ vendor_id }} = 4 then 'Unknown Vendor'
end
{% endmacro %}