{# This macro `relationship_except` is designed to test the referential integrity between two tables (or models) 
   while allowing for exceptions (certain values to not consider).
   It checks if there are any orphaned records in a child table that do not have a 
   corresponding record in a parent table, based on a specified column. It also allows excluding specific values 
   from this check. #}

{% macro test_relationship_except(model, column_name, to, except) %}

    {# Define CTEs for parent and child tables/models #}
    with
        parent as (
            {# Select the specified column from the parent table/model #}
            select {{ to }}.{{ column_name }} as parent_key from {{ to }}
        ),

        child as (
            {# Select the specified column from the child table/model, excluding nulls and optionally specified values #}
            select {{ model }}.{{ column_name }} as child_key
            from {{ model }}
            where
                {{ model }}.{{ column_name }} is not null
                {% if except %}
                    {# Exclude specified values if `except` parameter is provided #}
                    and {{ model }}.{{ column_name }}
                    not in ('{{ except | join("', '") }}')
                {% endif %}
        )

    {# Main query to find orphaned child records #}
    select child.child_key
    from child
    left join parent on child.child_key = parent.parent_key
    where parent.parent_key is null

{% endmacro %}
