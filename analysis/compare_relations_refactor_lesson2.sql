{# in dbt Develop #}

{% set old_etl_relation=ref('legacy_customer_orders_lesson2') %}
{% set dbt_relation=ref('fct_customer_orders_lesson2') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="order_id"
) }}

