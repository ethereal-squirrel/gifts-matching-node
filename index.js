require('dotenv').config();
const mysql = require('mysql');

/*
  Initialise the database db.
*/

/*
  Initialise the do not match array.
*/
var dnm = {};
var dnmArray = [];

var countMatched = 0;
var countUnmatched = 0;
var matchList = [];

/*
  A simple function to check if a user is in the do not match array, and if so, if they are colliding with the user we're checking.
*/
const doNotMatch = (user_id, match_id) => {
  if (!dnm[user_id]) {
    return false;
  };

  if (dnm[user_id].indexOf(match_id) > -1) {
    return true;
  };

  return false;
};

/*
  A simple function to shuffle an array.
*/
function shuffle(array) {
  let currentIndex = array.length, randomIndex;

  while (currentIndex != 0) {
    randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex--;

    [array[currentIndex], array[randomIndex]] = [
      array[randomIndex], array[currentIndex]];
  }

  return array;
};

/*
  The primary matching loop.
*/
async function match(exchangeId) {
  const db = mysql.createConnection({
    host: process.env.SQL_HOST,
    user: process.env.SQL_USER,
    password: process.env.SQL_PASSWORD,
    database: process.env.SQL_DATABASE
  });
  
  console.log(`Attempting matching loop for #${exchangeId}`);
  console.log('Connecting to database...');
  db.connect();

  //populateDatabase();

  console.time("execution");

  var groups = {};
  var users = [];

  var matched = 0;
  var unmatched = 0;

  console.log('Querying database...');
  console.log('----------------------------------------');

  await new Promise((resolve, reject) => {
    db.query('SELECT * FROM DoNotMatch', function (error, results, fields) {
      results.map((row) => {
        dnmArray.push(row['FirstUserId']);
        dnmArray.push(row['SecondUserId']);

        if (!dnm[row['FirstUserId']]) {
          dnm[row['FirstUserId']] = [];
        };

        if (!dnm[row['SecondUserId']]) {
          dnm[row['SecondUserId']] = [];
        };

        dnm[row['FirstUserId']].push(row['SecondUserId']);
        dnm[row['SecondUserId']].push(row['FirstUserId']);
      });

      console.log(`Found ${Object.keys(dnm).length} entries in DoNotMatch table.`);

      resolve();
    });
  });

  await new Promise((resolve, reject) => {
    db.query('SELECT * FROM MatchesNew WHERE ExchangeId = ' + exchangeId, function (error, results, fields) {
      if (error) throw error;

      console.log(`Found ${results.length} users to match.`);

      results.map(row => {
        let user = {
          user_id: row.UserId,
          matching_group: row.MatchingGroup,
          send_to: row.SendTo
        };

        if (!groups[user.matching_group]) {
          groups[user.matching_group] = [];
        };

        groups[user.matching_group].push(user);
      });

      console.log(`Found ${Object.keys(groups).length} matching groups.`);
      console.log('----------------------------------------');

      Object.keys(groups).map((group) => {
        console.log(`Attempting to match ${group}...`);

        if (groups[group].length == 1) {
          // If there's only one user in the group, we can't match them.

          unmatched++;
          console.log(`Only one user in ${group}. Unable to match.`);
        } else if (groups[group].length == 2) {
          // If there's only two users in the group, we can match them with one another.

          if (!doNotMatch(groups[group][0].user_id, groups[group][1].user_id)) {
            users.push({
              user_id: groups[group][0].user_id,
              matched_with: groups[group][1].user_id
            });

            users.push({
              user_id: groups[group][1].user_id,
              matched_with: groups[group][0].user_id
            });

            matched = matched + 2;

            console.log('Users Matched: 2/2 [Enforced two-way match.]');
          } else {
            // If the two users are in the do not match table, we can't match them.

            unmatched = unmatched + 2;

            console.log('Unable to two-way match, do not match is present. Skipping group.');
          };
        } else if (groups[group].length === 3) {
          // If there's only three users in the group, we can match them with one another.

          if (!doNotMatch(groups[group][0].user_id, groups[group][1].user_id) && !doNotMatch(groups[group][0].user_id, groups[group][2].user_id) && !doNotMatch(groups[group][1].user_id, groups[group][2].user_id)) {
            users.push({
              user_id: groups[group][0].user_id,
              matched_with: groups[group][1].user_id
            });

            users.push({
              user_id: groups[group][1].user_id,
              matched_with: groups[group][2].user_id
            });

            users.push({
              user_id: groups[group][2].user_id,
              matched_with: groups[group][0].user_id
            });

            matched = matched + 3;

            console.log('Users Matched: 3/3 [Enforced three-way match.]');
          } else {
            // If there is a do not match collision, we'll attempt a pairing between two of them.

            if (!doNotMatch(groups[group][0].user_id, groups[group][1].user_id)) {
              users.push({
                user_id: groups[group][0].user_id,
                matched_with: groups[group][1].user_id
              });

              users.push({
                user_id: groups[group][1].user_id,
                matched_with: groups[group][0].user_id
              });

              matched = matched + 2;
              unmatched++;

              console.log('Users Matched: 2/3 [Unable to three-way match.]');
            } else if (!doNotMatch(groups[group][1].user_id, groups[group][2].user_id)) {
              users.push({
                user_id: groups[group][1].user_id,
                matched_with: groups[group][2].user_id
              });

              users.push({
                user_id: groups[group][2].user_id,
                matched_with: groups[group][1].user_id
              });

              matched = matched + 2;
              unmatched++;

              console.log('Users Matched: 2/3 [Unable to three-way match.]');
            } else if (!doNotMatch(groups[group][0].user_id, groups[group][2].user_id)) {
              users.push({
                user_id: groups[group][0].user_id,
                matched_with: groups[group][2].user_id
              });

              users.push({
                user_id: groups[group][2].user_id,
                matched_with: groups[group][0].user_id
              });

              matched = matched + 2;
              unmatched++;

              console.log('Users Matched: 2/3 [Unable to three-way match.]');
            } else {
              // If there is a do not match collision across all three users, we can't match them.

              unmatched = unmatched + 3;

              console.log('Unable to three-way match, do not match is present across all users. Skipping group.');
            }
          };
        } else {
          // If there's more than three users in the group, we'll attempt to match them using our group function.

          let result = matchGroup(groups[group]);
          console.log('Users Matched: ' + result.matched + ' / ' + (result.matched + result.unmatched));

          matched = matched + result.matched;
          unmatched = unmatched + result.unmatched;
          users = users.concat(result.matchedUsers);
        };

        console.log('----------------------------------------');
      });

      console.log(`Matched Users: ${matched}`);
      console.log(`Unmatched Users: ${unmatched}`);

      console.log('----------------------------------------');

      console.log(`Matching complete. [#${exchangeId}]`);
      console.timeEnd("execution");

      countMatched = matched;
      countUnmatched = unmatched;
      matchList = users;

      var sql = "DELETE FROM MatchesNew WHERE ExchangeId = " + exchangeId;

      db.query(sql, [], function (err) {
        if (err) throw err;

        console.log(`Matching complete. [#${exchangeId}]`);
        console.timeEnd("execution");
        db.end();

        resolve();
      });
    });
  });
};

/*
  Match a group of users.
*/
function matchGroup(group) {
  var groupUsers = [];
  var groupMatched = 0;
  var groupUnmatched = 0;
  var matchedUsers = [];

  groupUsers = shuffle(group);

  var validDnm = false;
  var dnmCount = 0;

  groupUsers.map((user) => {
    if (dnmArray.includes(user.user_id)) {
      validDnm = true;
      dnmCount++;
    };
  });

  if (dnmCount <= 1) {
    validDnm = false;
  };

  if (validDnm === false) {
    /*
      If there are no do not match entries for any of the users in the group, we can match them all in a progressive manner.
    */

    groupUsers.map((user, index) => {
      var i;

      if (index == (groupUsers.length - 1)) {
        i = 0;
      } else {
        i = (index + 1);
      };

      matchedUsers.push({
        user_id: groupUsers[index].user_id,
        matched_with: groupUsers[i].user_id
      });

      groupMatched++;
    });
  } else {
    /*
      If a do not match is present, we need to use smart logic to avoid collisions.
      Well, that would be the smart approach.
      Instead, we're just going to brute force it.

      @TODO Make this smarter.
    */

    console.log('Valid DNM entries found, running alternative matching.');

    var c = true;
    var currentUsers = [];
    var currentMatched = [];
    var currentUnmatched = [];
    var i = 1;

    while (c === true) {
      console.log(`Attempting matching loop #${i}.`);

      groupUsers = shuffle(groupUsers);
      currentUsers = [];
      currentMatched = [];
      currentUnmatched = [];

      groupUsers.map((user, index) => {
        var currentIdx;

        if (index == (groupUsers.length - 1)) {
          currentIdx = 0;
        } else {
          currentIdx = index + 1;
        };

        if (!doNotMatch(user.user_id, groupUsers[currentIdx].user_id)) {
          currentUsers.push({
            user_id: user.user_id,
            matched_with: groupUsers[currentIdx].user_id
          });

          currentMatched.push(groupUsers[currentIdx].user_id);
        } else {
          currentUnmatched.push(groupUsers[currentIdx].user_id);
        };
      });

      if (currentUnmatched.length == 0) {
        c = false;
        groupMatched = currentMatched.length;
        groupUnmatched = currentUnmatched.length;
      };

      i++;
    };

    matchedUsers = currentUsers;
  };

  return { matchedUsers: matchedUsers, matched: groupMatched, unmatched: groupUnmatched };
};

module.exports.handler = async (event, context, callback) => {
  let body = event.body.replace("'", '');
  let obj = JSON.parse(body);

  let exchangeId = obj.exchangeId;
  console.log(`Exchange ID: ${exchangeId}`);

  let output = await match(exchangeId);

  const response = {
    statusCode: 200,
    body: JSON.stringify({
      matched: countMatched,
      unmatched: countUnmatched,
      matchList: matchList
    }),
  };

  console.log(`Response...`);
  console.dir(response);

  callback(null, response);
  return response;
};
