{# in dbt Develop #}

{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="dbt_mdickson",
      identifier="legacy_customer_orders"
) -%}

{% set dbt_relation=ref('fct_customer_orders') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["loaded_at"],
    primary_key="order_id"
) }}