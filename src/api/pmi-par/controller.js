import {success, notFound} from '../../services/response/'
import {PmiPar} from '.'

const AthenaExpress = require("athena-express"),
  aws = require("aws-sdk"),
  awsCredentials = {
    region: "us-east-1",
    accessKeyId: "AKIAJFTF3EZFWQJCN3EQ",
    secretAccessKey: "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"
  };

aws.config.update(awsCredentials);

const athenaExpressConfig = {
  aws
};

var athena = require('athena-client')

var clientConfig = {
  bucketUri: 's3://samplepooja2'
}

var awsConfig = {
  accessKeyId: 'AKIAJFTF3EZFWQJCN3EQ',
  secretAccessKey: 'el7oKlgpNj5L4jix/VyxrYjmoHXTL68lMjXq9VEn',
  region: 'us-east-1',
}

const ATHENA = require('aws_athena_client');
var AWS = require('aws-sdk');
var athena1 = new AWS.Athena();

const CLIENT = ATHENA.Client().setOptions(
  {
    region: 'us-east-1',
    accessKeyId: 'AKIAJFTF3EZFWQJCN3EQ',
    secretAccessKey: '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'
  }
);

export const express = ({body}, res, next) => {

  let offset = 1;
  let limit = 20;
  let newLimit = 0;
  let newOffset = 0;
  let flag = "query";
  let where;
  let totalCountWithWhere;
  let condition = ' OR ';
  console.log("body", body);

  //var client = athena.createClient(clientConfig, awsConfig);
  let query1 = 'select row_number() over() AS Row_number,* from prvdr_reassign_data';

  if (body.condition) {
    condition = ' ' + body.condition + ' ';
  }

  let Key = [];
  for (let key in body) {
    if (key != "offset" && key != "limit" && key != "condition") {
      Key.push(key)
    }
  }

  if (Key.length > 0) {
    Key.forEach(function (key) {

      let string = body[key].toString();
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

  if (body.offset) {
    offset = parseInt(body.offset);
  }

  if (body.limit) {
    limit = parseInt(body.limit);
  }

  for (let i = 0; i < offset; i++) {
    newLimit += limit;
  }

  newOffset = newLimit - (limit - 1);
  console.log('newoffset', newOffset);
  console.log('newLimit', newLimit);

  query += ' where Row_number BETWEEN ' + newOffset + ' AND ' + newLimit;
  console.log('query', query);

  let countQury = 'SELECT count(*) over() AS Total FROM (' + query1 + ' ) limit 1';
  console.log('countQury', countQury);

  const athenaExpress = new AthenaExpress(athenaExpressConfig);
  athenaExpress
    .query(countQury)
    .then(record => {
      console.log("len", record.Items.length);
      if (record.Items.length > 0) {
        totalCountWithWhere = record.Items[0].Total
      } else {
        totalCountWithWhere = 0
      }
      athenaExpress
        .query(query)
        .then(records => {
          res.status(200).send({
            Message: "Get Details Successsfully",
            Total_Count: totalCountWithWhere,
            Count: records.Items.length,
            Status: "Success",
            data: records.Items,
          });
        })
        .catch(error => {
          console.log(error);
        });
    })
    .catch(error => {
      console.log(error);
    });

  /*client.execute(countQury)
    .toPromise()
    .then((record) => {
      console.log("len", record.records.length);
      if (record.records.length > 0) {
        totalCountWithWhere = record.records[0].Total
      } else {
        totalCountWithWhere = 0
      }
      console.log('TotalRecord', totalCountWithWhere)

      client.execute(query).toPromise()
        .then((record) => {
          console.log('see');
          res.status(200).send({
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
      res(err, null);
    })*/
}

export const expressWithoutcount = ({body}, res, next) => {

  let offset = 1;
  let limit = 20;
  let newLimit = 0;
  let newOffset = 0;
  let flag = "query";
  let where;
  let totalCountWithWhere;
  let condition = ' OR ';
  console.log("body", body);

  //var client = athena.createClient(clientConfig, awsConfig);
  let query1 = 'select row_number() over() AS Row_number,* from prvdr_reassign_data';

  if (body.condition) {
    condition = ' ' + body.condition + ' ';
  }

  let Key = [];
  for (let key in body) {
    if (key != "offset" && key != "limit" && key != "condition") {
      Key.push(key)
    }
  }

  if (Key.length > 0) {
    Key.forEach(function (key) {

      let string = body[key].toString();
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

  if (body.offset) {
    offset = parseInt(body.offset);
  }

  if (body.limit) {
    limit = parseInt(body.limit);
  }

  for (let i = 0; i < offset; i++) {
    newLimit += limit;
  }

  newOffset = newLimit - (limit - 1);
  console.log('newoffset', newOffset);
  console.log('newLimit', newLimit);

  query += ' where Row_number BETWEEN ' + newOffset + ' AND ' + newLimit;
  console.log('query', query);

  const athenaExpress = new AthenaExpress(athenaExpressConfig);
  athenaExpress
    .query(query)
    .then(records => {
      res.status(200).send({
        Message: "Get Details Successsfully",
        Count: records.Items.length,
        Status: "Success",
        data: records.Items,
      });
    })
    .catch(error => {
      console.log(error);
    });
}

export const client = ({body}, res, next) => {

  let offset = 1;
  let limit = 20;
  let newLimit = 0;
  let newOffset = 0;
  let flag = "query";
  let where;
  let totalCountWithWhere;
  let condition = ' OR ';
  console.log("body", body);

  var client = athena.createClient(clientConfig, awsConfig);
  let query1 = 'select row_number() over() AS Row_number,* from prvdr_reassign_data';

  if (body.condition) {
    condition = ' ' + body.condition + ' ';
  }

  let Key = [];
  for (let key in body) {
    if (key != "offset" && key != "limit" && key != "condition") {
      Key.push(key)
    }
  }

  if (Key.length > 0) {
    Key.forEach(function (key) {

      let string = body[key].toString();
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

  if (body.offset) {
    offset = parseInt(body.offset);
  }

  if (body.limit) {
    limit = parseInt(body.limit);
  }

  for (let i = 0; i < offset; i++) {
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
      console.log("len", record.records.length);
      if (record.records.length > 0) {
        totalCountWithWhere = record.records[0].Total
      } else {
        totalCountWithWhere = 0
      }
      console.log('TotalRecord', totalCountWithWhere)

      client.execute(query).toPromise()
        .then((record) => {
          console.log('see');
          res.status(200).send({
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
      res(err, null);
    })
}

export const clientWithoutcount = ({body}, res, next) => {

  let offset = 1;
  let limit = 20;
  let newLimit = 0;
  let newOffset = 0;
  let flag = "query";
  let where;
  let totalCountWithWhere;
  let condition = ' OR ';
  console.log("body", body);

  var client = athena.createClient(clientConfig, awsConfig);
  let query1 = 'select row_number() over() AS Row_number,* from prvdr_reassign_data';

  if (body.condition) {
    condition = ' ' + body.condition + ' ';
  }

  let Key = [];
  for (let key in body) {
    if (key != "offset" && key != "limit" && key != "condition") {
      Key.push(key)
    }
  }

  if (Key.length > 0) {
    Key.forEach(function (key) {

      let string = body[key].toString();
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

  if (body.offset) {
    offset = parseInt(body.offset);
  }

  if (body.limit) {
    limit = parseInt(body.limit);
  }

  for (let i = 0; i < offset; i++) {
    newLimit += limit;
  }

  newOffset = newLimit - (limit - 1);
  console.log('newoffset', newOffset);
  console.log('newLimit', newLimit);

  query += ' where Row_number BETWEEN ' + newOffset + ' AND ' + newLimit;
  console.log('query', query);

  let countQury = 'SELECT count(*) over() AS Total FROM (' + query1 + ' ) limit 1';
  console.log('countQury', countQury);

  client.execute(query).toPromise()
    .then((record) => {
      console.log('see');
      res.status(200).send({
        Message: "Get Details Successsfully",
        Count: record.records.length,
        Status: "Success",
        data: record.records,
      });

    }).catch((err) => {
    console.error(err)
  })

}

export const otherClient = ({body}, res, next) => {

  let offset = 1;
  let limit = 20;
  let newLimit = 0;
  let newOffset = 0;
  let flag = "query";
  let where;
  let totalCountWithWhere;
  let condition = ' OR ';
  console.log("body", body);

  //var client = athena.createClient(clientConfig, awsConfig);
  let query1 = 'select row_number() over() AS Row_number,* from prvdr_reassign_data';

  if (body.condition) {
    condition = ' ' + body.condition + ' ';
  }

  let Key = [];
  for (let key in body) {
    if (key != "offset" && key != "limit" && key != "condition") {
      Key.push(key)
    }
  }

  if (Key.length > 0) {
    Key.forEach(function (key) {

      let string = body[key].toString();
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

  if (body.offset) {
    offset = parseInt(body.offset);
  }

  if (body.limit) {
    limit = parseInt(body.limit);
  }

  for (let i = 0; i < offset; i++) {
    newLimit += limit;
  }

  newOffset = newLimit - (limit - 1);
  console.log('newoffset', newOffset);
  console.log('newLimit', newLimit);

  query += ' where Row_number BETWEEN ' + newOffset + ' AND ' + newLimit;
  console.log('query', query);

  let countQury = 'SELECT count(*) over() AS Total FROM (' + query1 + ' ) limit 1';
  console.log('countQury', countQury);

  var params = {
    QueryString: countQury,
    ResultConfiguration: {
      OutputLocation: 's3://samplepooja2',
    },
    QueryExecutionContext: {
      Database: 'prvdr_reassign_data'
    }
  };
  /*CLIENT.request(
    ATHENA.Request().setQuery(
      {
        QueryString: query,
        ResultConfiguration: {
          OutputLocation: "s3://samplepooja2/"
        }
      }
      // Set where you want the output to go.
      // Here it just goes to standard out.
    ).setOutput(console.log('see',process.stdout))
  );*/
  athena1.startQueryExecution(params, function (err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else console.log('see', data);           // successful response
  });

  /*client.execute(countQury)
    .toPromise()
    .then((record) => {
      console.log("len", record.records.length);
      if (record.records.length > 0) {
        totalCountWithWhere = record.records[0].Total
      } else {
        totalCountWithWhere = 0
      }
      console.log('TotalRecord', totalCountWithWhere)

      client.execute(query).toPromise()
        .then((record) => {
          console.log('see');
          res.status(200).send({
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
      res(err, null);
    })*/
}


