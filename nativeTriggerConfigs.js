// Name:    couponTest_upsertUserStats
// CRON:    45 11 6 10 *
// Function
exports = function() {
    // define collections
    const contents = context.services.get("dev-cluster").db("app-db").collection("contents");
    const userStats = context.services.get("dev-cluster").db("analytics-db").collection("userStats");

    // define aggregation cursor
    const extractContentByCreatorsCursor = [
        {
          '$match': {
            "createdAt": {
                '$gte': new Date(ISODate().getTime() - 1000*60*60*24*14)
                }
            }
        }, {
          '$group': {
            '_id': {
              '_id': '$author.id', 
              'hashtag': '$hashtags.payload', 
              'type': '$type'
            }, 
            'contentCount': {
              '$count': {}
            }, 
            'likeCount': {
              '$sum': '$engagements.like.count'
            }, 
            'commentCount': {
              '$sum': '$engagements.comment.count'
            }, 
            'recastCount': {
              '$sum': '$engagements.recast.count'
            }, 
            'quoteCount': {
              '$sum': '$engagements.quote.count'
            }
          }
        }, {
          '$group': {
            '_id': {
              '_id': '$_id._id', 
              'hashtag': '$_id.hashtag'
            }, 
            'hashtagContentCount': {
              '$sum': '$contentCount'
            }, 
            'hashtagLikeCount': {
              '$sum': '$likeCount'
            }, 
            'hashtagCommentCount': {
              '$sum': '$commentCount'
            }, 
            'hashtagRecastCount': {
              '$sum': '$recastCount'
            }, 
            'hashtagQuoteCount': {
              '$sum': '$quoteCount'
            }, 
            'contentSummary': {
              '$push': {
                'type': '$_id.type', 
                'typeCount': '$contentCount', 
                'likeCount': '$likeCount', 
                'commentCount': '$commentCount', 
                'recastCount': '$recastCount', 
                'quoteCount': '$quoteCount'
              }
            }
          }
        }, {
          '$group': {
            '_id': '$_id._id', 
            'userContentCount': {
              '$sum': '$hashtagContentCount'
            }, 
            'userLikeCount': {
              '$sum': '$hashtagLikeCount'
            }, 
            'userCommentCount': {
              '$sum': '$hashtagCommentCount'
            }, 
            'userRecastCount': {
              '$sum': '$hashtagRecastCount'
            }, 
            'userQuoteCount': {
              '$sum': '$hashtagQuoteCount'
            }, 
            'hastagSummary': {
              '$push': {
                'hashtag': '$_id.hashtag', 
                'hashtagContentCount': '$hashtagContentCount', 
                'hashtagLikeCount': '$hashtagLikeCount', 
                'hashtagCommentCount': '$hashtagCommentCount', 
                'hashtagRecastCount': '$hashtagRecastCount', 
                'hashtagQuoteCount': '$hashtagQuoteCount', 
                'contentSummary': '$contentSummary'
              }
            }
          }
        }, {
          '$lookup': {
            'from': 'users', 
            'localField': '_id', 
            'foreignField': '_id', 
            'as': 'userDetail'
          }
        }, {
          '$project': {
            '_id': 1, 
            'ownerAccount': {
              '$first': '$userDetail.ownerAccount'
            }, 
            'displayId': {
              '$first': '$userDetail.displayId'
            }, 
            'createdAt': {
              '$first': '$userDetail.createdAt'
            }, 
            'visibility': {
              '$first': '$userDetail.visibility'
            }, 
            'followedCount': {
              '$first': '$userDetail.followedCount'
            }, 
            'followerCount': {
              '$first': '$userDetail.followerCount'
            }, 
            'userContentCount': 1, 
            'userLikeCount': 1, 
            'userCommentCount': 1, 
            'userRecastCount': 1, 
            'userQuoteCount': 1, 
            'hastagSummary': 1
          }
        },{
          '$merge': {
            'into': {'db': "analytics-db", 'coll': "userStats"},
            'on': "_id",
            'whenMatched': "replace", 
            'whenNotMatched': "insert"
          }
        }, {
            '$merge': {
                'into': {
                    'db': 'analytics-db', 
                    'coll': 'userStats'
                }, 
                'on': '_id', 
                'whenMatched': 'replace', 
                'whenNotMatched': 'insert'
            }
        }
    ];

    // perform aggregation
    const result = contents.aggregate(extractContentByCreatorsCursor);

    // print the timestamp
    console.log(`${result.length} documents were updated at ${new Date()}`)

};