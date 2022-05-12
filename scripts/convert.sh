# Random samples for posts
yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.RandomSampling /user/stud/stackoverflow/landing/Posts /user/stud/stackoverflow/landing/posts_sample 2

# Convert XML to Sequence files
yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/Users /user/stud/stackoverflow/master/Users Id,Reputation,CreationDate,DisplayName,LastAccessDate,WebsiteUrl,Location,AboutMe,Views,UpVotes,DownVotes,ProfileImageUrl,AccountId,Age

yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/Posts /user/stud/stackoverflow/master/Posts Id,PostTypeId,AcceptedAnswerId,ParentId,CreationDate,DeletionDate,Score,ViewCount,OwnerUserId,OwnerDisplayName,LastEditorUserId,LastEditorDisplayName,LastEditDate,LastActivityDate,Title,Tags,AnswerCount,CommentCount,FavoriteCount,ClosedDate,CommunityOwnedDate

yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/Badges /user/stud/stackoverflow/master/Badges Id,UserId,Name,Date,Class,TagBased

yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/Comments /user/stud/stackoverflow/master/Comments Id,PostId,Score,CreationDate,UserDisplayName,UserId

yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/PostLinks /user/stud/stackoverflow/master/PostLinks Id,CreationDate,PostId,RelatedPostId,LinkTypeId

yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/Tags /user/stud/stackoverflow/master/Tags Id,TagName,Count,ExcerptPostId,WikiPostId

yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/Votes /user/stud/stackoverflow/master/Votes Id,PostId,VoteTypeId,UserId,CreationDate,BountyAmount

yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/posts_sample /user/stud/stackoverflow/master/posts_sample Id,PostTypeId,AcceptedAnswerId,ParentId,CreationDate,DeletionDate,Score,ViewCount,OwnerUserId,OwnerDisplayName,LastEditorUserId,LastEditorDisplayName,LastEditDate,LastActivityDate,Title,Tags,AnswerCount,CommentCount,FavoriteCount,ClosedDate,CommunityOwnedDate

yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/PostHistory /user/stud/stackoverflow/master/PostHistory Id,PostHistoryTypeId,PostId,RevisionGUID,CreationDate,UserId,UserDisplayName,Comment,Text



yarn jar mr-jobs-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.ConvertToSequenceFile /user/stud/stackoverflow/landing/Posts /user/stud/stackoverflow/master/Posts_tmp Id,PostTypeId,AcceptedAnswerId,ParentId,CreationDate,DeletionDate,Score,ViewCount,OwnerUserId,OwnerDisplayName,LastEditorUserId,LastEditorDisplayName,LastEditDate,LastActivityDate,Title,Tags,AnswerCount,CommentCount,FavoriteCount,ClosedDate,CommunityOwnedDate

hdfs dfs -rm -skipTrash /user/stud/stackoverflow/master/qqq