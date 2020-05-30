Review Guidelines
=================

Effective code review is a skill like any other professional skill you
develop with experience. Effective code review requires trust. No
one is perfect. Everyone makes mistakes. Trust builds over time.

This document will enumerate behaviors commonly observed and
associated with competent reviews of changes purposed to the Swift
code base. No one is expected to "follow these steps". Guidelines
are not *rules*, not all behaviors will be relevant in all situations.

    Code review is collaboration, not judgement.

    -- Alistair Coles

Checkout the Change
-------------------

You will need to have a copy of the change in an environment where you
can freely edit and experiment with the code in order to provide a
non-superficial review. Superficial reviews are not terribly helpful.
Always try to be helpful. ;)

Check out the change so that you may begin.

Commonly, ``git review -d <change-id>``

Run it
------

Imagine that you submit a patch to Swift, and a reviewer starts to
take a look at it. Your commit message on the patch claims that it
fixes a bug or adds a feature, but as soon as the reviewer downloads
it locally and tries to test it, a severe and obvious error shows up.
Something like a syntax error or a missing dependency.

"Did you even run this?" is the review comment all contributors dread.

Reviewers in particular need to be fearful merging changes that just
don't work - or at least fail in frequently common enough scenarios to
be considered "horribly broken". A comment in our review that says
roughly "I ran this on my machine and observed ``description of
behavior change is supposed to achieve``" is the most powerful defense
we have against the terrible scorn from our fellow Swift developers
and operators when we accidentally merge bad code.

If you're doing a fair amount of reviews - you will participate in
merging a change that will break my clusters - it's cool - I'll do it
to you at some point too (sorry about that). But when either of us go
look at the reviews to understand the process gap that allowed this to
happen - it better not be just because we were too lazy to check it out
and run it before it got merged.

Or be warned, you may receive, the dreaded...

    "Did you even *run* this?"

I'm sorry, I know it's rough. ;)

Consider edge cases very seriously
----------------------------------

    Saying "that should rarely happen" is the same as saying "that
    *will* happen"

    -- Douglas Crockford

Scale is an *amazingly* abusive partner. If you contribute changes to
Swift your code is running - in production - at scale - and your bugs
cannot hide. I wish on all of us that our bugs may be exceptionally
rare - meaning they only happen in extremely unlikely edge cases. For
example, bad things that happen only 1 out of every 10K times an op is
performed will be discovered in minutes. Bad things that happen only
1 out of every one billion times something happens will be observed -
by multiple deployments - over the course of a release. Bad things
that happen 1/100 times some op is performed are considered "horribly
broken". Tests must exhaustively exercise possible scenarios. Every
system call and network connection will raise an error and timeout -
where will that Exception be caught?

Run the tests
-------------

Yes, I know Gerrit does this already. You can do it *too*. You might
not need to re-run *all* the tests on your machine - it depends on the
change. But, if you're not sure which will be most useful - running
all of them best - unit - functional - probe. If you can't reliably
get all tests passing in your development environment you will not be
able to do effective reviews. Whatever tests/suites you are able to
exercise/validate on your machine against your config you should
mention in your review comments so that other reviewers might choose
to do *other* testing locally when they have the change checked out.

e.g.

    I went ahead and ran probe/test_object_metadata_replication.py on
    my machine with both sync_method = rsync and sync_method = ssync -
    that works for me - but I didn't try it with object_post_as_copy =
    false

Maintainable Code is Obvious
----------------------------

Style is an important component to review. The goal is maintainability.

However, keep in mind that generally style, readability and
maintainability are orthogonal to the suitability of a change for
merge. A critical bug fix may be a well written pythonic masterpiece
of style - or it may be a hack-y ugly mess that will absolutely need
to be cleaned up at some point - but it absolutely should merge
because: CRITICAL. BUG. FIX.

You should comment inline to praise code that is "obvious". You should
comment inline to highlight code that you found to be "obfuscated".

Unfortunately "readability" is often subjective. We should remember
that it's probably just our own personal preference. Rather than a
comment that says "You should use a list comprehension here" - rewrite
the code as a list comprehension, run the specific tests that hit the
relevant section to validate your code is correct, then leave a
comment that says:

    I find this more readable:

    ``diff with working tested code``

If the author (or another reviewer) agrees - it's possible the change will get
updated to include that improvement before it is merged; or it may happen in a
follow-up change.

However, remember that style is non-material - it is useful to provide (via
diff) suggestions to improve maintainability as part of your review - but if
the suggestion is functionally equivalent - it is by definition optional.

Commit Messages
---------------

Read the commit message thoroughly before you begin the review.

Commit messages must answer the "why" and the "what for" - more so
than the "how" or "what it does". Commonly this will take the form of
a short description:

- What is broken - without this change
- What is impossible to do with Swift - without this change
- What is slower/worse/harder - without this change

If you're not able to discern why a change is being made or how it
would be used - you may have to ask for more details before you can
successfully review it.

Commit messages need to have a high consistent quality. While many
things under source control can be fixed and improved in a follow-up
change - commit messages are forever. Luckily it's easy to fix minor
mistakes using the in-line edit feature in Gerrit!  If you can avoid
ever having to *ask* someone to change a commit message you will find
yourself an amazingly happier and more productive reviewer.

Also commit messages should follow the OpenStack Commit Message
guidelines, including references to relevant impact tags or bug
numbers. You should hand out links to the OpenStack Commit Message
guidelines *liberally* via comments when fixing commit messages during
review.

Here you go: `GitCommitMessages <https://wiki.openstack.org/wiki/GitCommitMessages#Summary_of_Git_commit_message_structure>`_

New Tests
---------

New tests should be added for all code changes. Historically you
should expect good changes to have a diff line count ratio of at least
2:1 tests to code. Even if a change has to "fix" a lot of *existing*
tests, if a change does not include any *new* tests it probably should
not merge.

If a change includes a good ratio of test changes and adds new tests -
you should say so in your review comments.

If it does not - you should write some!

... and offer them to the patch author as a diff indicating to them that
"something" like these tests I'm providing as an example will *need* to be
included in this change before it is suitable to merge. Bonus points if you
include suggestions for the author as to how they might improve or expand upon
the tests stubs you provide.

Be *very* careful about asking an author to add a test for a "small change"
before attempting to do so yourself. It's quite possible there is a lack of
existing test infrastructure needed to develop a concise and clear test - the
author of a small change may not be the best person to introduce a large
amount of new test infrastructure. Also, most of the time remember it's
*harder* to write the test than the change - if the author is unable to
develop a test for their change on their own you may prevent a useful change
from being merged. At a minimum you should suggest a specific unit test that
you think they should be able to copy and modify to exercise the behavior in
their change. If you're not sure if such a test exists - replace their change
with an Exception and run tests until you find one that blows up.

Documentation
-------------

Most changes should include documentation. New functions and code
should have Docstrings. Tests should obviate new or changed behaviors
with descriptive and meaningful phrases. New features should include
changes to the documentation tree. New config options should be
documented in example configs. The commit message should document the
change for the change log.

Always point out typos or grammar mistakes when you see them in
review, but also consider that if you were able to recognize the
intent of the statement - documentation with typos may be easier to
iterate and improve on than nothing.

If a change does not have adequate documentation it may not be suitable to
merge. If a change includes incorrect or misleading documentation or is
contrary to *existing* documentation is probably is not suitable to merge.

Every change could have better documentation.

Like with tests, a patch isn't done until it has docs. Any patch that
adds a new feature, changes behavior, updates configs, or in any other
way is different than previous behavior requires docs. manpages,
sample configs, docstrings, descriptive prose in the source tree, etc.

Reviewers Write Code
--------------------

Reviews have been shown to provide many benefits - one of which is shared
ownership. After providing a positive review you should understand how the
change works. Doing this will probably require you to "play with" the change.

You might functionally test the change in various scenarios. You may need to
write a new unit test to validate the change will degrade gracefully under
failure. You might have to write a script to exercise the change under some
superficial load. You might have to break the change and validate the new
tests fail and provide useful errors. You might have to step through some
critical section of the code in a debugger to understand when all the possible
branches are exercised in tests.

When you're done with your review an artifact of your effort will be
observable in the piles of code and scripts and diffs you wrote while
reviewing. You should make sure to capture those artifacts in a paste
or gist and include them in your review comments so that others may
reference them.

e.g.

    When I broke the change like this:

    ``diff``

    it blew up like this:

    ``unit test failure``


It's not uncommon that a review takes more time than writing a change -
hopefully the author also spent as much time as you did *validating* their
change but that's not really in your control. When you provide a positive
review you should be sure you understand the change - even seemingly trivial
changes will take time to consider the ramifications.

Leave Comments
--------------

Leave. Lots. Of. Comments.

A popular web comic has stated that
`WTFs/Minute <http://www.osnews.com/images/comics/wtfm.jpg>`_ is the
*only* valid measurement of code quality.

If something initially strikes you as questionable - you should jot
down a note so you can loop back around to it.

However, because of the distributed nature of authors and reviewers
it's *imperative* that you try your best to answer your own questions
as part of your review.

Do not say "Does this blow up if it gets called when xyz" - rather try
and find a test that specifically covers that condition and mention it
in the comment so others can find it more quickly. Or if you can find
no such test, add one to demonstrate the failure, and include a diff
in a comment. Hopefully you can say "I *thought* this would blow up,
so I wrote this test, but it seems fine."

But if your initial reaction is "I don't understand this" or "How does
this even work?" you should notate it and explain whatever you *were*
able to figure out in order to help subsequent reviewers more quickly
identify and grok the subtle or complex issues.

Because you will be leaving lots of comments - many of which are
potentially not highlighting anything specific - it is VERY important
to leave a good summary. Your summary should include details of how
you reviewed the change. You may include what you liked most, or
least.

If you are leaving a negative score ideally you should provide clear
instructions on how the change could be modified such that it would be
suitable for merge - again diffs work best.

Scoring
-------

Scoring is subjective. Try to realize you're making a judgment call.

A positive score means you believe Swift would be undeniably better
off with this code merged than it would be going one more second
without this change running in production immediately. It is indeed
high praise - you should be sure.

A negative score means that to the best of your abilities you have not
been able to your satisfaction, to justify the value of a change
against the cost of its deficiencies and risks. It is a surprisingly
difficult chore to be confident about the value of unproven code or a
not well understood use-case in an uncertain world, and unfortunately
all too easy with a **thorough** review to uncover our defects, and be
reminded of the risk of... regression.

Reviewers must try *very* hard first and foremost to keep master stable.

If you can demonstrate a change has an incorrect *behavior* it's
almost without exception that the change must be revised to fix the
defect *before* merging rather than letting it in and having to also
file a bug.

Every commit must be deployable to production.

Beyond that - almost any change might be merge-able depending on
its merits!  Here are some tips you might be able to use to find more
changes that should merge!

#. Fixing bugs is HUGELY valuable - the *only* thing which has a
   higher cost than the value of fixing a bug - is adding a new
   bug - if it's broken and this change makes it fixed (without
   breaking anything else) you have a winner!

#. Features are INCREDIBLY difficult to justify their value against
   the cost of increased complexity, lowered maintainability, risk
   of regression, or new defects. Try to focus on what is
   *impossible* without the feature - when you make the impossible
   possible, things are better. Make things better.

#. Purely test/doc changes, complex refactoring, or mechanical
   cleanups are quite nuanced because there's less concrete
   objective value. I've seen lots of these kind of changes
   get lost to the backlog. I've also seen some success where
   multiple authors have collaborated to "push-over" a change
   rather than provide a "review" ultimately resulting in a
   quorum of three or more "authors" who all agree there is a lot
   of value in the change - however subjective.

Because the bar is high - most reviews will end with a negative score.

However, for non-material grievances (nits) - you should feel
confident in a positive review if the change is otherwise complete
correct and undeniably makes Swift better (not perfect, *better*). If
you see something worth fixing you should point it out in review
comments, but when applying a score consider if it *need* be fixed
before the change is suitable to merge vs. fixing it in a follow up
change?  Consider if the change makes Swift so undeniably *better*
and it was deployed in production without making any additional
changes would it still be correct and complete?  Would releasing the
change to production without any additional follow up make it more
difficult to maintain and continue to improve Swift?

Endeavor to leave a positive or negative score on every change you review.

Use your best judgment.

A note on Swift Core Maintainers
--------------------------------

Swift Core maintainers may provide positive reviews scores that *look*
different from your reviews - a "+2" instead of a "+1".

But it's *exactly the same* as your "+1".

It means the change has been thoroughly and positively reviewed. The
only reason it's different is to help identify changes which have
received multiple competent and positive reviews. If you consistently
provide competent reviews you run a *VERY* high risk of being
approached to have your future positive review scores changed from a
"+1" to "+2" in order to make it easier to identify changes which need
to get merged.

Ideally a review from a core maintainer should provide a clear path
forward for the patch author. If you don't know how to proceed
respond to the reviewers comments on the change and ask for help.
We'd love to try and help.
