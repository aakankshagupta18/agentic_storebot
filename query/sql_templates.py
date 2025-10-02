from jinja2 import Template

SELECT_TEMPLATE = Template("""
SELECT {{ cols|join(", ") }}
FROM {{ fq_table }}
{% if where %}WHERE {{ where }}{% endif %}
{% if order_by %}ORDER BY {{ order_by }}{% endif %}
{% if limit %}LIMIT {{ limit }}{% endif %}
""")

