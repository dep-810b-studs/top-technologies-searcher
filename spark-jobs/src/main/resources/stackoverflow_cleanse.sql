-- remove dangling references to non-existing answers
update POSTS set ACCEPTEDANSWERID = null where ID in (
  select ID from POSTS where ACCEPTEDANSWERID not in (select ID from POSTS)
  );

-- remove pending flags referenced to non-existing posts (because of misaligned data)
delete from PendingFlags where POSTID not in (select ID from posts);

delete from PostFeedback where POSTID not in (select ID from posts);

delete from POSTLINKS where POSTID not in (select ID from posts);

delete from POSTLINKS where RELATEDPOSTID not in (select ID from posts);

delete from PostNotices where POSTID not in(select ID from POSTS);

delete from POSTTAGS where POSTID not in (select ID from POSTS);

delete from ReviewTaskResults where REVIEWTASKID not in (select id from REVIEWTASKS);

-- select count(*) from ReviewTasks where COMPLETEDBYREVIEWTASKID not in (select id from REVIEWTASKS);

delete from SuggestedEdits where POSTID not in (select id from posts);

delete from SUGGESTEDEDITVOTES where SUGGESTEDEDITID in (
      select id from SuggestedEdits where POSTID not in (select id from posts)
      );

delete from SUGGESTEDEDITS where OWNERUSERID not in (select id from users);

delete from VOTES where POSTID not in (select id from POSTS);

commit;