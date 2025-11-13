{% macro normalize_email(email) %}
    {#
    Normalize email addresses to consistent format.
    
    Rules:
    - Lowercase
    - Trim whitespace
    - Return NULL if empty/invalid
    
    Examples:
    - "John.Doe@Example.COM" → "john.doe@example.com"
    - "  user@email.com  " → "user@email.com"
    #}
    
    case
        when {{ email }} is null or trim({{ email }}) = '' then null
        when {{ email }} not like '%@%' then null
        when position('@' in {{ email }}) = 1 then null
        when position('@' in {{ email }}) = length({{ email }}) then null
        else lower(trim({{ email }}))
    end
{% endmacro %}

