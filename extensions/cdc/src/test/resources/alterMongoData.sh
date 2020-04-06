mongo -u debezium -p dbz --authenticationDatabase admin localhost:27017/inventory <<-EOF
    use inventory;

    db.customers.insertOne(
        { _id : NumberLong("1005"), first_name : 'Jason', last_name : 'Bourne', email : 'jason@bourne.org' }
    );

    db.customers.updateOne( { last_name : "Kretchmar" }, { \$set: { first_name : "Anne Marie"} } );

    db.customers.deleteOne( { 'last_name' : 'Bourne' } );
EOF
