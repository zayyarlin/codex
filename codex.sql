CREATE TABLE package (
  id varchar(100) NOT NULL,
  metadata jsonb
);


select metadata->>'resources' as resources, metadata->>'name' as package_name from package

select * from
(
select 
    jsonb_array_elements(metadata->'resources')->>'url' as resource_url,
    jsonb_array_elements(metadata->'resources')->>'format' as resource_format, 
    jsonb_array_elements(metadata->'resources')->>'title' as resource_title, 
    jsonb_array_elements(metadata->'resources')->>'description' as resource_desc, 
    jsonb_array_elements(metadata->'resources')->'schema' as resource_schema, 
    metadata->>'name' as package_name,
    metadata->>'description' as package_description
from package
) t where t.resource_format = 'CSV'