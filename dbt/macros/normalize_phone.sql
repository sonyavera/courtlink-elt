{% macro normalize_phone(phone) %}
    {#
    Normalize phone numbers to consistent format - ALL will have + prefix.
    
    Rules:
    - US numbers (10 digits): Add +1 prefix → +1XXXXXXXXXX
    - US numbers (11 digits starting with 1): Add + prefix → +1XXXXXXXXXX
    - Already formatted as +1XXXXXXXXXX: Keep as-is
    - International numbers (starting with + but not +1): Keep as-is
    - Invalid/empty: Return NULL
    
    Examples:
    - "+12345678901" → "+12345678901"
    - "12345678901" → "+12345678901"
    - "2345678901" → "+12345678901"
    - "+442071234567" → "+442071234567" (international)
    - "(234) 567-8901" → "+12345678901"
    #}
    
    case
        when {{ phone }} is null or trim({{ phone }}::text) = '' then null
        -- International number (starts with + but not +1) - keep as-is with +
        when {{ phone }}::text like '+%' 
             and not ({{ phone }}::text like '+1%') 
        then {{ phone }}::text
        -- Already in +1XXXXXXXXXX format (has + and 11 digits starting with 1) - keep as-is
        when {{ phone }}::text like '+1%' 
             and length(regexp_replace({{ phone }}::text, '[^0-9]', '', 'g')) = 11
             and regexp_replace({{ phone }}::text, '[^0-9]', '', 'g') like '1%'
        then {{ phone }}::text
        -- 11 digits starting with 1: add + prefix
        when length(regexp_replace({{ phone }}::text, '[^0-9]', '', 'g')) = 11 
             and regexp_replace({{ phone }}::text, '[^0-9]', '', 'g') like '1%' 
        then '+' || regexp_replace({{ phone }}::text, '[^0-9]', '', 'g')
        -- 10 digits: add +1 prefix
        when length(regexp_replace({{ phone }}::text, '[^0-9]', '', 'g')) = 10 
        then '+1' || regexp_replace({{ phone }}::text, '[^0-9]', '', 'g')
        -- More than 10 digits: take last 10 and add +1 prefix
        when length(regexp_replace({{ phone }}::text, '[^0-9]', '', 'g')) > 10 
        then '+1' || right(regexp_replace({{ phone }}::text, '[^0-9]', '', 'g'), 10)
        -- Invalid format
        else null
    end
{% endmacro %}
