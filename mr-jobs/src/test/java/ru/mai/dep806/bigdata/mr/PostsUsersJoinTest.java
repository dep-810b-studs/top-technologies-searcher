package ru.mai.dep806.bigdata.mr;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;

/**
 * Test for PostsUsersJoin Reducer.
 */
public class PostsUsersJoinTest {

    private static final String postRowString1 = "<row Id=\"4\" PostTypeId=\"1\" AcceptedAnswerId=\"7\" CreationDate=\"2008-07-31T21:42:52.667\" Score=\"421\" ViewCount=\"28370\" Body=\"&lt;p&gt;I want to use a track-bar to change a form's opacity.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;This is my code:&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;decimal trans = trackBar1.Value / 5000;&#xA;this.Opacity = trans;&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;When I try to build it, I get this error:&lt;/p&gt;&#xA;&#xA;&lt;blockquote&gt;&#xA;  &lt;p&gt;Cannot implicitly convert type 'decimal' to 'double'.&lt;/p&gt;&#xA;&lt;/blockquote&gt;&#xA;&#xA;&lt;p&gt;I tried making &lt;code&gt;trans&lt;/code&gt; a &lt;code&gt;double&lt;/code&gt;, but then the control doesn't work. This code has worked fine for me in VB.NET in the past. &lt;/p&gt;&#xA;\" OwnerUserId=\"8\" LastEditorUserId=\"5455605\" LastEditorDisplayName=\"Rich B\" LastEditDate=\"2015-12-23T21:34:28.557\" LastActivityDate=\"2016-07-17T20:33:18.217\" Title=\"When setting a form's opacity should I use a decimal or double?\" Tags=\"&lt;c#&gt;&lt;winforms&gt;&lt;type-conversion&gt;&lt;decimal&gt;&lt;opacity&gt;\" AnswerCount=\"13\" CommentCount=\"3\" FavoriteCount=\"33\" CommunityOwnedDate=\"2012-10-31T16:42:47.213\" />";
    private static final String postRowString2 = "<row Id=\"5\" PostTypeId=\"1\" AcceptedAnswerId=\"7\" CreationDate=\"2008-07-31T21:42:52.667\" Score=\"421\" ViewCount=\"28370\" Body=\"&lt;p&gt;I want to use a track-bar to change a form's opacity.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;This is my code:&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;decimal trans = trackBar1.Value / 5000;&#xA;this.Opacity = trans;&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;When I try to build it, I get this error:&lt;/p&gt;&#xA;&#xA;&lt;blockquote&gt;&#xA;  &lt;p&gt;Cannot implicitly convert type 'decimal' to 'double'.&lt;/p&gt;&#xA;&lt;/blockquote&gt;&#xA;&#xA;&lt;p&gt;I tried making &lt;code&gt;trans&lt;/code&gt; a &lt;code&gt;double&lt;/code&gt;, but then the control doesn't work. This code has worked fine for me in VB.NET in the past. &lt;/p&gt;&#xA;\" OwnerUserId=\"8\" LastEditorUserId=\"5455605\" LastEditorDisplayName=\"Rich B\" LastEditDate=\"2015-12-23T21:34:28.557\" LastActivityDate=\"2016-07-17T20:33:18.217\" Title=\"When setting a form's opacity should I use a decimal or double?\" Tags=\"&lt;c#&gt;&lt;winforms&gt;&lt;type-conversion&gt;&lt;decimal&gt;&lt;opacity&gt;\" AnswerCount=\"13\" CommentCount=\"3\" FavoriteCount=\"33\" CommunityOwnedDate=\"2012-10-31T16:42:47.213\" />";
    private static final String userRowString = "<row Id=\"8\" Reputation=\"38745\" CreationDate=\"2008-07-31T14:22:31.287\" DisplayName=\"Jeff Atwood\" LastAccessDate=\"2016-09-03T01:29:40.997\" WebsiteUrl=\"http://www.codinghorror.com/blog/\" Location=\"El Cerrito, CA\" AboutMe=\"&lt;p&gt;&lt;a href=&quot;http://www.codinghorror.com/blog/archives/001169.html&quot; rel=&quot;nofollow&quot;&gt;Stack Overflow Valued Associate #00001&lt;/a&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Wondering how our software development process works? &lt;a href=&quot;http://www.youtube.com/watch?v=08xQLGWTSag&quot; rel=&quot;nofollow&quot;&gt;Take a look!&lt;/a&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Find me &lt;a href=&quot;http://twitter.com/codinghorror&quot; rel=&quot;nofollow&quot;&gt;on twitter&lt;/a&gt;, or &lt;a href=&quot;http://www.codinghorror.com/blog&quot; rel=&quot;nofollow&quot;&gt;read my blog&lt;/a&gt;. Don't say I didn't warn you &lt;em&gt;because I totally did&lt;/em&gt;.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;However, &lt;a href=&quot;http://www.codinghorror.com/blog/2012/02/farewell-stack-exchange.html&quot; rel=&quot;nofollow&quot;&gt;I no longer work at Stack Exchange, Inc&lt;/a&gt;. I'll miss you all. Well, &lt;em&gt;some&lt;/em&gt; of you, anyway. :)&lt;/p&gt;&#xA;\" Views=\"227781\" UpVotes=\"3304\" DownVotes=\"1304\" ProfileImageUrl=\"https://www.gravatar.com/avatar/51d623f33f8b83095db84ff35e15dbe8?s=128&amp;amp;d=identicon&amp;amp;r=PG\" Age=\"46\" AccountId=\"1\" />";

    @Test
    public void testReducer() throws Exception {
        Map<String, String> postRow1 = XmlUtils.parseXmlRow(postRowString1);
        Map<String, String> postRow2 = XmlUtils.parseXmlRow(postRowString2);
        Map<String, String> userRow = XmlUtils.parseXmlRow(userRowString);

        String post1 = PostsUsersJoin.concatenateFields(new StringBuilder(), postRow1, 'P', PostsUsersJoin.POST_FIELDS);
        String post2 = PostsUsersJoin.concatenateFields(new StringBuilder(), postRow2, 'P', PostsUsersJoin.POST_FIELDS);
        String user = PostsUsersJoin.concatenateFields(new StringBuilder(), userRow, 'U', PostsUsersJoin.USER_FIELDS);

        String joined1 = PostsUsersJoin.concatenateFields(new StringBuilder(), postRow1, '\0', PostsUsersJoin.POST_FIELDS) +
                PostsUsersJoin.concatenateFields(new StringBuilder(), userRow, '\0', PostsUsersJoin.USER_FIELDS);
        String joined2 = PostsUsersJoin.concatenateFields(new StringBuilder(), postRow2, '\0', PostsUsersJoin.POST_FIELDS) +
                PostsUsersJoin.concatenateFields(new StringBuilder(), userRow, '\0', PostsUsersJoin.USER_FIELDS);

        PostsUsersJoin.JoinReducer reducer = new PostsUsersJoin.JoinReducer();
        LongWritable key = new LongWritable(8);

        List<String> outputValuesToVerify = new ArrayList<>();
        PostsUsersJoin.JoinReducer.Context context = Mockito.mock(PostsUsersJoin.JoinReducer.Context.class);
        doAnswer((invocationOnMock) -> {
            outputValuesToVerify.add(invocationOnMock.getArguments()[1].toString());
            return null;
        }).when(context).write(any(), any());

        reducer.reduce(key, Arrays.asList(new Text(post1), new Text(user), new Text(post2)), context);

        Assert.assertEquals(2, outputValuesToVerify.size());
        Assert.assertEquals(joined1, outputValuesToVerify.get(0));
        Assert.assertEquals(joined2, outputValuesToVerify.get(1));
        Mockito.verify(context, times(2)).write(Mockito.eq(key), any());
        Mockito.verifyNoMoreInteractions(context);

        String[] values = joined1.split("\01");
        String[] fields = (String[]) ArrayUtils.addAll(PostsUsersJoin.POST_FIELDS, PostsUsersJoin.USER_FIELDS);
        for (int i = 0; i < fields.length; i++) {
            System.out.println(fields[i] + ": " + values[i]);
        }
    }

}