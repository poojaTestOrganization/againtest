var athena = require("athena-client")

var clientConfig = {
    bucketUri: 's3://samplepooja2'
}

var awsConfig = {
    accessKeyId: process.env.accessKeyId,
    secretAccessKey: process.env.secretAccessKey,
    region: 'us-east-1',
}
exports.handler = function (event, context, callback) {

    let offset = 1;
    let limit = 20;
    let newLimit = 0;
    let newOffset = 0;
    let flag = "query";
    let where;
    let totalCountWithWhere;
    let condition = ' OR ';

    console.log("event", event);
    var client = athena.createClient(clientConfig, awsConfig);
    let query1 = 'select row_number() over() AS Row_number,* from pmi_par';

    if (event.params.condition) {
        condition = ' ' + event.params.condition + ' ';
    }

    let Key = [];
    for (key in event.params) {
        if (key != "offset" && key != "limit" && key != "condition") {
            Key.push(key)
        }
    }

    if (Key.length > 0) {
        Key.forEach(function (key) {

            let string = event.params[key];
            let array = string.split(',');
            console.log("array", array);

            let str = "(";
            for (let x = 0; x < array.length; x++) {
                if (x === array.length - 1) {
                    str += "'" + array[x] + "'";
                } else {
                    str += "'" + array[x] + "',";
                }
            }
            str += ")";
            console.log("array.length", Key.length);


            if (isNaN(array[0])) {
                console.log("HERE");
                if (!where) {
                    where = " where " + key + ' IN ' + str;
                }
                else {
                    where += condition + key + ' IN ' + str;
                }
            }
            else {
                console.log("HERE1");
                let str = "("
                for (let x = 0; x < array.length; x++) {

                    if (x === array.length - 1) {
                        str += array[x];
                    } else {
                        str += array[x] + ",";
                    }
                }
                str += ")";
                if (!where) {

                    where = " where " + key + ' IN ' + str;
                }
                else {
                    where += condition + key + ' IN ' + str;
                }
            }

        });
        if (Key.toString() != "offset") {
            query1 += where
            flag = "query1";
        }
    }

    let query = 'SELECT * FROM (' + query1 + ')';

    if (event.params.offset) {
        offset = parseInt(event.params.offset);
    }

    if (event.params.limit) {
        limit = parseInt(event.params.limit);
    }

    for (i = 0; i < offset; i++) {
        newLimit += limit;
    }

    newOffset = newLimit - (limit - 1);
    console.log('newoffset', newOffset);
    console.log('newLimit', newLimit);

    query += ' where Row_number BETWEEN ' + newOffset + ' AND ' + newLimit;
    console.log('query', query);

    let countQury = 'SELECT count(*) over() AS Total FROM (' + query1 + ' ) limit 1';
    console.log('countQury', countQury);

    client.execute(countQury)
        .toPromise()
        .then((record) => {
            console.log("rec", record);
            console.log("len", record.records.length);
            if (record.records.length > 0) {
                totalCountWithWhere = record.records[0].Total
            } else {
                totalCountWithWhere = 0
            }
            console.log('TotalRecord', totalCountWithWhere)
            client.execute(query).toPromise()
                .then((record) => {
                    callback(null, {
                        Message: "Get Details Successsfully",
                        Total_Count: totalCountWithWhere,
                        Count: record.records.length,
                        Status: "Success",
                        data: record.records,
                    });

                }).catch((err) => {
                console.error(err)
            })
        })
        .catch((err) => {
            console.log('err', err);
            callback(err, null);
        })

};
