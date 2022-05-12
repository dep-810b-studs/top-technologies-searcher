create table Posts(
    Id int,
    PostTypeId int,
    AcceptedAnswerId int,
    ParentId int,
    CreationDate date,
    DeletionDate date,
    Score int,
    ViewCount int,
    Body CLOB,
    OwnerUserId int,
    OwnerDisplayName varchar2 (40 CHAR),
    LastEditorUserId int,
    LastEditorDisplayName varchar2 (40 CHAR),
    LastEditDate date,
    LastActivityDate date,
    Title varchar2 (500 CHAR),
    Tags varchar2 (250 CHAR),
    AnswerCount int,
    CommentCount int,
    FavoriteCount int,
    ClosedDate date,
    CommunityOwnedDate date
);

create table Users(
    Id int,
    Reputation int,
    CreationDate date,
    DisplayName varchar2 (40 CHAR),
    LastAccessDate date,
    WebsiteUrl varchar2 (400 CHAR),
    Location varchar2 (200 CHAR),
    AboutMe CLOB,
    Views int,
    UpVotes int,
    DownVotes int,
    ProfileImageUrl varchar2 (200 CHAR),
    EmailHash varchar2 (32 CHAR),
    AccountId int
);

create table Comments(
    Id int,
    PostId int,
    Score int,
    Text varchar2 (4000 CHAR),
    CreationDate date,
    UserDisplayName varchar2 (30 CHAR),
    UserId int
);

create table Badges(
    Id int,
    UserId int,
    Name varchar2 (50 CHAR),
    "Date" date,
    Class int,
    TagBased int
);

create table CloseAsOffTopicReasonTypes(
    Id smallint,
    IsUniversal int,
    MarkdownMini varchar2 (500 CHAR),
    CreationDate date,
    CreationModeratorId int,
    ApprovalDate date,
    ApprovalModeratorId int,
    DeactivationDate date,
    DeactivationModeratorId int
);

create table CloseReasonTypes(
    Id int,
    Name varchar2 (200 CHAR),
    Description varchar2 (500 CHAR)
);

create table FlagTypes(
    Id int,
    Name varchar2 (50 CHAR),
    Description varchar2 (500 CHAR)
);

create table PendingFlags(
    Id int,
    FlagTypeId int,
    PostId int,
    CreationDate date,
    CloseReasonTypeId int,
    CloseAsOffTopicReasonTypeId int,
    DuplicateOfQuestionId int,
    BelongsOnBaseHostAddress varchar2 (100 CHAR)
);

create table PostFeedback(
    Id int,
    PostId int,
    IsAnonymous int,
    VoteTypeId int,
    CreationDate date
);

create table PostHistory(
    Id int,
    PostHistoryTypeId int,
    PostId int,
    RevisionGUID varchar2(64 CHAR),
    CreationDate date,
    UserId int,
    UserDisplayName varchar2 (40 CHAR),
    "Comment" varchar2 (4000 CHAR),
    Text CLOB
);

create table PostHistoryTypes(
    Id int,
    Name varchar2 (50 CHAR)
);

create table PostLinks(
    Id int,
    CreationDate date,
    PostId int,
    RelatedPostId int,
    LinkTypeId int
);

create table PostNotices(
    Id int,
    PostId int,
    PostNoticeTypeId int,
    CreationDate date,
    DeletionDate date,
    ExpiryDate date,
    Body CLOB,
    OwnerUserId int,
    DeletionUserId int
);

create table PostNoticeTypes(
    Id int,
    ClassId int,
    Name varchar2 (80 CHAR),
    Body CLOB,
    IsHidden int,
    Predefined int,
    PostNoticeDurationId int
);

create table PostsWithDeleted(
    Id int,
    PostTypeId int,
    AcceptedAnswerId int,
    ParentId int,
    CreationDate date,
    DeletionDate date,
    Score int,
    ViewCount int,
    Body CLOB,
    OwnerUserId int,
    OwnerDisplayName varchar2 (40 CHAR),
    LastEditorUserId int,
    LastEditorDisplayName varchar2 (40 CHAR),
    LastEditDate date,
    LastActivityDate date,
    Title varchar2 (250 CHAR),
    Tags varchar2 (250 CHAR),
    AnswerCount int,
    CommentCount int,
    FavoriteCount int,
    ClosedDate date,
    CommunityOwnedDate date
);

create table PostTags(
    PostId int,
    TagId int
);

create table PostTypes(
    Id int,
    Name varchar2 (50 CHAR)
);

create table ReviewRejectionReasons(
    Id int,
    Name varchar2 (100 CHAR),
    Description varchar2 (300 CHAR),
    PostTypeId int
);

create table ReviewTaskResults(
    Id int,
    ReviewTaskId int,
    ReviewTaskResultTypeId int,
    CreationDate date,
    RejectionReasonId int,
    "Comment" varchar2 (150 CHAR)
);

create table ReviewTaskResultTypes(
    Id int,
    Name varchar2 (100 CHAR),
    Description varchar2 (300 CHAR)
);

create table ReviewTasks(
    Id int,
    ReviewTaskTypeId int,
    CreationDate date,
    DeletionDate date,
    ReviewTaskStateId int,
    PostId int,
    SuggestedEditId int,
    CompletedByReviewTaskId int
);

create table ReviewTaskStates(
    Id int,
    Name varchar2 (50 CHAR),
    Description varchar2 (300 CHAR)
);

create table ReviewTaskTypes(
    Id int,
    Name varchar2 (50 CHAR),
    Description varchar2 (300 CHAR)
);

create table SuggestedEdits(
    Id int,
    PostId int,
    CreationDate date,
    ApprovalDate date,
    RejectionDate date,
    OwnerUserId int,
    "Comment" varchar2 (800 CHAR),
    Text CLOB,
    Title varchar2 (250 CHAR),
    Tags varchar2 (250 CHAR),
    RevisionGUID varchar2 (64 CHAR)
);

create table SuggestedEditVotes(
    Id int,
    SuggestedEditId int,
    UserId int,
    VoteTypeId int,
    CreationDate date,
    TargetUserId int,
    TargetRepChange int
);

create table Tags(
    Id int,
    TagName varchar2 (35 CHAR),
    Count int,
    ExcerptPostId int,
    WikiPostId int
);

create table TagSynonyms(
    Id int,
    SourceTagName varchar2 (35 CHAR),
    TargetTagName varchar2 (35 CHAR),
    CreationDate date,
    OwnerUserId int,
    AutoRenameCount int,
    LastAutoRename date,
    Score int,
    ApprovedByUserId int,
    ApprovalDate date
);

create table Votes(
    Id int,
    PostId int,
    VoteTypeId int,
    UserId int,
    CreationDate date,
    BountyAmount int
);

create table VoteTypes(
    Id int,
    Name varchar2 (50 CHAR)
);

alter table POSTS add constraint PK_POSTS primary key (ID);
alter table USERS add constraint PK_USERS primary key (ID);
alter table COMMENTS add constraint PK_COMMENTS primary key (ID);
alter table BADGES add constraint PK_BADGES primary key (ID);
alter table CLOSEASOFFTOPICREASONTYPES add constraint PK_CLOSEASOFFTOPICREASONTYPES primary key (ID);
alter table CLOSEREASONTYPES add constraint PK_CLOSEREASONTYPES primary key (ID);
alter table FLAGTYPES add constraint PK_FLAGTYPES primary key (ID);
alter table PENDINGFLAGS add constraint PK_PENDINGFLAGS primary key (ID);
alter table POSTFEEDBACK add constraint PK_POSTFEEDBACK primary key (ID);
alter table POSTHISTORY add constraint PK_POSTHISTORY primary key (ID);
alter table POSTHISTORYTYPES add constraint PK_POSTHISTORYTYPES primary key (ID);
alter table POSTLINKS add constraint PK_POSTLINKS primary key (ID);
alter table POSTNOTICES add constraint PK_POSTNOTICES primary key (ID);
alter table POSTNOTICETYPES add constraint PK_POSTNOTICETYPES primary key (ID);
alter table POSTSWITHDELETED add constraint PK_POSTSWITHDELETED primary key (ID);
alter table POSTTAGS add constraint PK_POSTTAGS primary key (POSTID, TAGID);
alter table POSTTYPES add constraint PK_POSTTYPES primary key (ID);
alter table REVIEWREJECTIONREASONS add constraint PK_REVIEWREJECTIONREASONS primary key (ID);
alter table REVIEWTASKRESULTS add constraint PK_REVIEWTASKRESULTS primary key (ID);
alter table REVIEWTASKRESULTTYPES add constraint PK_REVIEWTASKRESULTTYPES primary key (ID);
alter table REVIEWTASKS add constraint PK_REVIEWTASKS primary key (ID);
alter table REVIEWTASKSTATES add constraint PK_REVIEWTASKSTATES primary key (ID);
alter table REVIEWTASKTYPES add constraint PK_REVIEWTASKTYPES primary key (ID);
alter table SUGGESTEDEDITS add constraint PK_SUGGESTEDEDITS primary key (ID);
alter table SUGGESTEDEDITVOTES add constraint PK_SUGGESTEDEDITVOTES primary key (ID);
alter table TAGS add constraint PK_TAGS primary key (ID);
alter table TAGSYNONYMS add constraint PK_TAGSYNONYMS primary key (ID);
alter table VOTES add constraint PK_VOTES primary key (ID);
alter table VOTETYPES add constraint PK_VOTETYPES primary key (ID);

create index I_POSTS_ACCEPTEDANSWERID on POSTS(ACCEPTEDANSWERID);

alter table POSTS add constraint FK_POSTS_PostTypeId foreign key(PostTypeId) references POSTTYPES(ID);
alter table POSTS add constraint FK_POSTS_AcceptedAnswerId foreign key(AcceptedAnswerId) references POSTS(ID);
alter table POSTS add constraint FK_POSTS_ParentId foreign key(ParentId) references POSTS(Id);
alter table POSTS add constraint FK_POSTS_OwnerUserId foreign key(OwnerUserId) references USERS(Id);
alter table POSTS add constraint FK_POSTS_LastEditorUserId foreign key(LastEditorUserId) references USERS(Id);

alter table Comments add constraint FK_Comments_PostId foreign key(PostId) references POSTS(ID);
alter table Comments add constraint FK_Comments_UserId foreign key(UserId) references USERS(ID);
alter table Badges add constraint FK_Badges_UserId foreign key(UserId) references USERS(ID);


alter table CloseAsOffTopicReasonTypes add constraint FK_OffTopRsnTypes_CrModerId
   foreign key(CreationModeratorId) references USERS(ID);

 alter table CloseAsOffTopicReasonTypes add constraint FK_OffTopRsnTypes_ApprModerId
   foreign key(ApprovalModeratorId) references USERS(ID);

 alter table PendingFlags add constraint FK_PendingFlags_FlagTypeId
   foreign key(FlagTypeId) references FlagTypes(ID);

alter table PendingFlags add constraint FK_PendingFlags_PostId
  foreign key(PostId) references POSTS(ID);

alter table PostFeedback add constraint FK_PostFeedback_PostId
  foreign key(PostId) references POSTS(ID);
alter table PostFeedback add constraint FK_PostFeedback_VoteTypeId
   foreign key(VoteTypeId) references VoteTypes(ID);

 alter table PostHistory add constraint FK_PostHist_PostHistTypeId
   foreign key(PostHistoryTypeId) references PostHistoryTypes(ID);

alter table PostHistory add constraint FK_PostHistory_PostId
   foreign key(PostId) references Posts(ID);

alter table PostHistory add constraint FK_PostHistory_UserId
   foreign key(UserId) references Users(ID);

alter table PostLinks add constraint FK_PostLinks_PostId
  foreign key(PostId) references Posts(ID);

alter table PostLinks add constraint FK_PostLinks_RelatedPostId
  foreign key(RelatedPostId) references Posts(ID);

-- !!!
-- alter table PostLinks add constraint FK_PostLinks_LinkTypeId
--    foreign key(LinkTypeId) references ???(ID);

alter table PostNotices add constraint FK_PostNotices_PostId
  foreign key(PostId) references Posts(ID);

alter table PostNotices add constraint FK_PostNot_PostNotTypeId
   foreign key(PostNoticeTypeId) references PostNoticeTypes(ID);



alter table PostsWithDeleted add constraint FK_PostsWithDeleted_PostTypeId foreign key(PostTypeId) references POSTTYPES(ID);
-- alter table PostsWithDeleted add constraint FK_PostsWD_AcceptedAnswerId foreign key(AcceptedAnswerId) references PostsWithDeleted(ID);
-- alter table PostsWithDeleted add constraint FK_PostsWD_ParentId foreign key(ParentId) references PostsWithDeleted(Id);
alter table PostsWithDeleted add constraint FK_PostsWD_OwnerUserId foreign key(OwnerUserId) references USERS(Id);
alter table PostsWithDeleted add constraint FK_PostsWD_LastEditorUserId foreign key(LastEditorUserId) references USERS(Id);

alter table PostTags add constraint FK_PostTags_PostId foreign key(PostId) references Posts(Id);
alter table PostTags add constraint FK_PostTags_TagId foreign key(TagId) references Tags(Id);

alter table ReviewTaskResults add constraint FK_ReviewTaskRes_ReviewTaskId
      foreign key(ReviewTaskId) references ReviewTasks(Id);

alter table ReviewTaskResults add constraint FK_RevTaskRes_RevTaskResTypeId
       foreign key(ReviewTaskResultTypeId) references ReviewTaskResultTypes(Id);

alter table ReviewTaskResults add constraint FK_RevTaskRes_RejReasonId
       foreign key(RejectionReasonId) references ReviewRejectionReasons(Id);

alter table ReviewTasks add constraint FK_RevTasks_RevTaskTypeId
       foreign key(ReviewTaskTypeId) references ReviewTaskTypes(Id);

alter table ReviewTasks add constraint FK_RevTasks_RevTaskStateId
       foreign key(ReviewTaskStateId) references ReviewTaskStates(Id);

alter table ReviewTasks add constraint FK_RevTasks_SuggEditId
      foreign key(SuggestedEditId) references SuggestedEdits(Id);

-- !!!
-- alter table ReviewTasks add constraint FK_RevTasks_ComplByRevTaskId
--      foreign key(CompletedByReviewTaskId) references ReviewTasks(Id);

alter table SuggestedEdits add constraint FK_SuggestedEdits_PostId
      foreign key(PostId) references Posts(Id);

alter table SuggestedEdits add constraint FK_SuggestedEdits_OwnerUserId
       foreign key(OwnerUserId) references Users(Id);

alter table ReviewRejectionReasons add constraint FK_RevRejReasons_PostTypeId foreign key(PostTypeId) references PostTypes(Id);

alter table SuggestedEditVotes add constraint FK_SuggEditVotes_SuggEditId
      foreign key(SuggestedEditId) references SuggestedEdits(Id);

alter table SuggestedEditVotes add constraint FK_SuggestedEditVotes_UserId
       foreign key(UserId) references Users(Id);

alter table SuggestedEditVotes add constraint FK_SuggEditVotes_VoteTypeId
       foreign key(VoteTypeId) references VoteTypes(Id);

alter table SuggestedEditVotes add constraint FK_SuggEditVotes_TargetUserId
       foreign key(TargetUserId) references Users(Id);

alter table Tags add constraint FK_Tags_ExcerptPostId
      foreign key(ExcerptPostId) references Posts(Id);

alter table Tags add constraint FK_Tags_WikiPostId
      foreign key(WikiPostId) references Posts(Id);

alter table TagSynonyms add constraint FK_TagSynonyms_OwnerUserId
      foreign key(OwnerUserId) references USERS(Id);

alter table TagSynonyms add constraint FK_TagSynonyms_ApprByUserId
      foreign key(ApprovedByUserId) references Users(Id);

alter table Votes add constraint FK_Votes_PostId
      foreign key(PostId) references Posts(Id);
alter table Votes add constraint FK_Votes_VoteTypeId
       foreign key(VoteTypeId) references VoteTypes(Id);

alter table Votes add constraint FK_Votes_UserId
      foreign key(UserId) references Users(Id);
