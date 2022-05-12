-- Id,Reputation,CreationDate,DisplayName,LastAccessDate,WebsiteUrl,Location,AboutMe,Views,UpVotes,DownVotes,ProfileImageUrl,AccountId,Age
CREATE EXTERNAL TABLE users(
	Id BIGINT,  
	Reputation INT, 
	CreationDate STRING,  
	DisplayName STRING, 
	LastAccessDate STRING, 
	WebsiteUrl STRING,
	Location STRING, 
	AboutMe STRING,
	Views INT, 
	UpVotes INT, 
	DownVotes INT, 
	ProfileImageUrl STRING,
	AccountId INT,
	Age INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' 
	STORED AS SEQUENCEFILE
	LOCATION '/user/stud/stackoverflow/master/users';

-- Id,PostTypeId,AcceptedAnswerId,ParentId,CreationDate,DeletionDate,Score,ViewCount,OwnerUserId,OwnerDisplayName,LastEditorUserId,LastEditorDisplayName,LastEditDate,LastActivityDate,Title,Tags,AnswerCount,CommentCount,FavoriteCount,ClosedDate,CommunityOwnedDate
-- Records count: 37215531
CREATE EXTERNAL TABLE posts (
	Id bigint, 
	PostTypeId int, 
	AcceptedAnswerId bigint, 
	ParentId bigint, 
	CreationDate string, 
	DeletionDate string, 
	Score bigint, 
	ViewCount bigint, 
	OwnerUserId bigint, 
	OwnerDisplayName string, 
	LastEditorUserId bigint, 
	LastEditorDisplayName string, 
	LastEditDate string, 
	LastActivityDate string, 
	Title string, 
	Tags string, 
	AnswerCount int, 
	CommentCount int, 
	FavoriteCount int, 
	ClosedDate string, 
	CommunityOwnedDate string 
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
	STORED AS SEQUENCEFILE 
	LOCATION '/user/stud/stackoverflow/master/posts';
	
CREATE EXTERNAL TABLE posts_sample (
	Id bigint, 
	PostTypeId int, 
	AcceptedAnswerId bigint, 
	ParentId bigint, 
	CreationDate string, 
	DeletionDate string, 
	Score bigint, 
	ViewCount bigint, 
	OwnerUserId bigint, 
	OwnerDisplayName string, 
	LastEditorUserId bigint, 
	LastEditorDisplayName string, 
	LastEditDate string, 
	LastActivityDate string, 
	Title string, 
	Tags string, 
	AnswerCount int, 
	CommentCount int, 
	FavoriteCount int, 
	ClosedDate string, 
	CommunityOwnedDate string 
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
	STORED AS SEQUENCEFILE 
	LOCATION '/user/stud/stackoverflow/master/posts_sample';

	
-- Id,UserId,Name,Date,Class,TagBased
CREATE EXTERNAL TABLE badges (
	Id bigint, 
	UserId bigint, 
	Name string, 
	Date string, 
	Class int, 
	TagBased string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
	STORED AS SEQUENCEFILE 
	LOCATION '/user/stud/stackoverflow/master/badges';

-- Id,PostId,Score,CreationDate,UserDisplayName,UserId
CREATE EXTERNAL TABLE comments (
	Id bigint, 
	PostId bigint, 
	Score int, 
	CreationDate string,
	UserDisplayName string,
	UserId bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
	STORED AS SEQUENCEFILE 
	LOCATION '/user/stud/stackoverflow/master/comments';
	
-- Id,CreationDate,PostId,RelatedPostId,LinkTypeId
CREATE EXTERNAL TABLE postlinks (
	Id bigint, 
	CreationDate string,
	PostId bigint, 
	RelatedPostId bigint, 
	LinkTypeId int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
	STORED AS SEQUENCEFILE 
	LOCATION '/user/stud/stackoverflow/master/postlinks';
	
-- Id,TagName,Count,ExcerptPostId,WikiPostId
CREATE EXTERNAL TABLE tags (
	Id bigint,
	TagName	string,
	Count int,
	ExcerptPostId bigint, 
	WikiPostId bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
	STORED AS SEQUENCEFILE 
	LOCATION '/user/stud/stackoverflow/master/Tags';
	
	
-- Id,PostId,VoteTypeId,UserId,CreationDate,BountyAmount
CREATE EXTERNAL TABLE votes (
	Id bigint,
	PostId bigint,
	VoteTypeId int,
	UserId bigint,
	CreationDate string,
	BountyAmount int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
	STORED AS SEQUENCEFILE 
	LOCATION '/user/stud/stackoverflow/master/votes';

CREATE EXTERNAL TABLE tagsynonyms (
  id INT,
  SourceTagName	STRING,
  TargetTagName	STRING,
  CreationDate	STRING,
  OwnerUserId	INT,
  AutoRenameCount	INT,
  LastAutoRename	STRING,
  Score	INT,
  ApprovedByUserId	INT,
  ApprovalDate STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/user/stud/stackoverflow/landing/TagSynonyms/';


CREATE EXTERNAL TABLE posts_users(
                               Id BIGINT,
                               Reputation INT,
                               CreationDate STRING,
                               DisplayName STRING,
                               LastAccessDate STRING,
                               WebsiteUrl STRING,
                               Location STRING,
                               AboutMe STRING,
                               Views INT,
                               UpVotes INT,
                               DownVotes INT,
                               ProfileImageUrl STRING,
                               AccountId INT,
                               Age INT,

                               Id bigint,
                               PostTypeId int,
                               AcceptedAnswerId bigint,
                               ParentId bigint,
                               CreationDate string,
                               DeletionDate string,
                               Score bigint,
                               ViewCount bigint,
                               OwnerUserId bigint,
                               OwnerDisplayName string,
                               LastEditorUserId bigint,
                               LastEditorDisplayName string,
                               LastEditDate string,
                               LastActivityDate string,
                               Title string,
                               Tags string,
                               AnswerCount int,
                               CommentCount int,
                               FavoriteCount int,
                               ClosedDate string,
                               CommunityOwnedDate string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS SEQUENCEFILE
    LOCATION '/user/stud/eugene/UserPostJoin';

--  "Id", "PostTypeId", "AcceptedAnswerId", "ParentId" , "CreationDate", "DeletionDate",
--                 "Score", "ViewCount", "Body" , "OwnerUserId", "OwnerDisplayName", "LastEditorUserId",
--                 "LastEditorDisplayName", "LastEditDate", "LastActivityDate", "Title", "Tags", "AnswerCount",
--                 "CommentCount", "FavoriteCount", "ClosedDate", "CommunityOwnedDate"
CREATE EXTERNAL TABLE filtered_posts (
                                Id bigint,
                                PostTypeId int,
                                AcceptedAnswerId bigint,
                                ParentId bigint,
                                CreationDate string,
                                DeletionDate string,
                                Score bigint,
                                ViewCount bigint,
                                Body string,
                                OwnerUserId bigint,
                                OwnerDisplayName string,
                                LastEditorUserId bigint,
                                LastEditorDisplayName string,
                                LastEditDate string,
                                LastActivityDate string,
                                Title string,
                                Tags string,
                                AnswerCount int,
                                CommentCount int,
                                FavoriteCount int,
                                ClosedDate string,
                                CommunityOwnedDate string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
    STORED AS SEQUENCEFILE
    LOCATION '/user/stud/eugene/PostsByTag2';
