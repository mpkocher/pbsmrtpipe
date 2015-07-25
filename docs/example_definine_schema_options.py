import pbsmrtpipe.schema_opt_utils as OP
# generate a task option id
oid = OP.to_opt_id('dev.hello_message')
# generate a schema
s = OP.to_option_schema(oid, "string", 'Hello Message', "Hello Message for dev Task.", "Default Message")
print {oid: s}
