# Act on DDM with each caller's own login

Status: proposed November 2, 2026. The requirement that a client without a
login sees only unmanaged devices is the user's decision and is accepted; the
mechanism below waits for the user's trial.

A DDM login from any browser was saved to the daemon's configuration and
used for every request. In a lab with a DDM 1.9 server and users holding
Read Only, Media Control and Domain Control rights in different domains, a
browser that never logged in renamed a managed device with another user's
rights, one user's login replaced another's domains, any browser's logout cut
everyone off, and the selected domain was shared. Any web page could also
post to the daemon and read the reply. Reported as GitHub #69, which proposed
an opt-in session mode that checks permissions periodically. The user's
direction: "no login = show unmanaged devices".

Every request has a caller. Requests from the daemon's own machine and MCP
requests act as the owner, keep the saved DDM account, and see everything.
The saved account feeds the shared managed inventory and DDM signals; a Read
Only account is enough, because other callers never write with it. A login
from another machine creates an in-memory session holding that user's DDM
token, sent as a `__Host-` cookie that is Secure, HttpOnly and
SameSite=Strict, and only over HTTPS. Sessions end after twelve idle hours or
seven days, and the store holds at most 256. Logout ends only that session,
and each session keeps its own selected domain.

GraphQL, domain changes, enrollment and managed device control run with the
caller's own token, so DDM decides what each user may do and its refusal is
returned as is. The daemon does not copy DDM's role model. A session sees the
domains DDM reported for its token at login or at an explicit refresh; there
is no periodic permission check. A caller without a login sees unmanaged
devices, including devices DDM knows but has not enrolled, and no managed
devices or domains.

The user's further direction: "you should be able to login and manage
multiple connections to multiple DDM servers". A browser can be logged in to
several DDM servers at once and log out of each separately. Logging in to a
server the daemon does not know yet adds it to the saved list, with its name
and address only; the browser's token stays in its session. A server with no
saved owner account gets its device list and DDM signals from a logged-in
browser's login, preferring the one that sees the most domains, and moves to
another browser's login when that one logs out or its session ends. That
borrowed login only reads; the owner cannot write with it, and every other
caller still writes with its own. Editing or removing a saved server remains
the owner's.

Requests other than GET, HEAD and OPTIONS are refused when Sec-Fetch-Site is
not same-origin or none, or, without it, when Origin does not match the
request's own scheme and authority. Native clients send neither. Responses no
longer carry `Access-Control-Allow-Origin: *`.

This changes the public API described in 0006: a caller without a login gets
403 from managed operations and filtered device, domain and event results.
The owner's behavior is unchanged. A session's view of a domain lags a DDM
role change until the user logs in again or refreshes. Without a saved owner
account, a user who sees domains the borrowed login cannot see does not get
those domains' devices; a saved Read Only account covering the site avoids
this. Any browser that can log in to a DDM server can add it to the list
everyone sees, so the list shows each server's address. Journal, preset,
metering and diagnostics routes are not yet filtered by caller, and the iOS
app's DDM login has not been tried with sessions.
