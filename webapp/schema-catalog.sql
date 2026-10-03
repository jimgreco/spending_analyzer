SELECT 'table' AS kind, c.relname::text AS name,
 json_build_array(c.relkind::text,c.relpersistence::text,c.relrowsecurity,c.relforcerowsecurity)::text AS definition
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND c.relkind IN ('r','p') AND c.relname<>'app_schema_versions'
UNION ALL
SELECT 'sequence', c.relname,
 json_build_array(format_type(s.seqtypid,NULL),s.seqincrement,s.seqmin,s.seqmax,s.seqcache,s.seqcycle)::text
FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public'
UNION ALL
SELECT 'column' AS kind, c.relname || '.' || a.attname AS name,
 json_build_array(format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid))::text AS definition
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
WHERE n.nspname='public' AND c.relkind='r' AND c.relname<>'app_schema_versions'
UNION ALL
SELECT 'constraint', c.relname || ':' || pg_get_constraintdef(k.oid), k.convalidated::text
FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND c.relname<>'app_schema_versions'
UNION ALL
SELECT 'index', indexname, indexdef FROM pg_indexes WHERE schemaname='public' AND tablename<>'app_schema_versions'
UNION ALL
SELECT 'function', p.oid::regprocedure::text, pg_get_functiondef(p.oid)
FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
AND p.prokind='f' AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e')
UNION ALL
SELECT 'trigger', c.relname || '.' || t.tgname, t.tgenabled::text || ':' || pg_get_triggerdef(t.oid)
FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND NOT t.tgisinternal
UNION ALL
SELECT 'extension', extname, extnamespace::regnamespace::text FROM pg_extension
ORDER BY kind,name;
