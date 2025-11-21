//! This example is about the ability to cancel a coroutine that dispatches
//! all I/O to other coroutines (and thus that exclusively blocks on `.await`,
//! rather than directly writing/reading or sleeping, for example).
//!
//! Currently canceling the dispatching coroutine has no effect as it never
//! gets to observe the cancellation flag.
//!
//! Things to consider:
//! - Making `future.await` unblock on cancellation either by propagating the cancellation
//!   implicitly to the awaited coroutine, or by just unblocking the call to `await`.
//! - Having `io.async` and `io.concurrent` (and same for `Group` equivalents) fail
//!   with `error.Canceled` (although this by itself is not a full solution).
//! - Add the concept of cancel-safe critical sections within a coroutine.
//!
//! After running the main example, read the comments and code at the bottom of this file
//! for more variations of this use case and some proposed solutions (it should be
//! noted that the proposed solutions are just starting points, not full solutions).

const std = @import("std");
const Io = std.Io;

const gpa = std.heap.smp_allocator;

pub fn main() !void {
    var threaded: Io.Threaded = .init(gpa);
    defer threaded.deinit();

    const io = threaded.io();

    var future = try io.concurrent(loopALot, .{io});
    defer future.cancel(io) catch {};

    std.debug.print("main: sleeping 2s then cancel\n", .{});
    try io.sleep(.fromSeconds(2), .real);
    std.debug.print("main: cancel (this should stop loopALot)!\n", .{});
    try future.cancel(io);
}

// Loops a lot.
fn loopALot(io: Io) !void {
    for (0..10) |i| {
        std.debug.print("loopALot: sleep {}\n", .{i});

        // We start another coroutine and await its result.
        // In this case we're calling io.sleep inside of the spawned coroutine,
        // but it doesn't really matter what it is that we do in there, this is
        // just a way of making it do "work".
        //
        // If you swap the concurrent/await lines with the currently commented
        // `io.sleep` line, the cancellation request from main will instead be
        // honored.
        var future = try io.concurrent(Io.sleep, .{ io, .fromSeconds(1), .real });
        try future.await(io);
        // try io.sleep(.fromSeconds(1), .real);
    }
}

// Here's another variant of the example above. Imagine that `loopALot` is now
// the route handler of a web server and that, to handle a client request,
// a database query is necessary.
//
// Here's one way of implementing this:
fn requestHandler(io: Io, db: anytype) !void {
    _ = io;
    const results = try db.query("SELECT FROM bla bla");
    _ = results; //write response
}

// If we assume that `db` does not spawn another coroutine then, as we saw
// in the main example, if cancellation happens while the query is being sent,
// then the TCP read/write will fail, and the result will be a corrupted
// connection (that either has in it a half-sent request, or a half-read
// reply).
//
// This would suck regardless, but it would suck even more if the `db`
// connection were to be multiplexed across different coroutines, as
// that would cause all other pending requests to fail, due to the data
// corruption.
//
// There are multiple ways of solving this. One could be to enable
// some coroutine-level cancellation shield:
fn requestHandler1(io: Io, db: anytype) !void {
    io.cancelShieldBegin();
    const results = try db.query("SELECT FROM bla bla");
    io.cancelShieldEnd();
    _ = results; //write response
}
// Another would be to require users to use coroutines as the atomic unit
// of cancellation, meaning that if you want `db.query` to not cancel when
// `requestHandler` is cancelled, then you would need to `io.concurrent` the
// call, like so:
fn requestHandler2(io: Io, db: anytype) !void {
    const future = try io.concurrent(@TypeOf(db).query, .{ db, "SELECT FROM bla bla" });
    const results = try future.await(io);
    _ = results; //write response

    // Also note how this approach *REQUIRES* usage of `io.concurrent`,
    // since `io.async` could decide to run the callback inline, which
    // would fail to protect the database connection.
}
// But to support this last approach you would need to:
// 1. unblock `requestHandler` from `future.await` *WITHOUT* canceling the
//    database query.
// 2. allow `requestHandler` to re-await the completion of the spawned
//    coroutine, if desirable (in this example it is desirable, but in other
//    cases it might not be).
//
// Here's what this could look like:
fn requestHandler3(io: Io, db: anytype) !void {
    const future = try io.concurrent(@TypeOf(db).query, .{ db, "SELECT FROM bla bla" });
    const results = future.await(io) catch |err| switch (err) {
        error.Canceled => {
            // we still want to await this
            try future.await();
            return error.Canceled;
        },
    };
    _ = results; //write response
}

// Two more things to note about this last example.
// One is that `error.Canceled` is a bad error name because, given current
// semantics, seeing `future.await` return it normally means that `future` got
// canceled while in fact this is *NOT* the case (and is in fact the thing we're
// trying to prevent in a sense).
//
// Another thing is that it would be nice if this system could still interact
// nicely with timeouts. Something along those lines:
fn requestHandler4(io: Io, db: anytype) !void {
    const future = try io.concurrent(@TypeOf(db).query, .{ db, "SELECT FROM bla bla" });
    const results = future.await(io) catch |err| switch (err) {
        error.Canceled => {
            // we still want to await this at least for a while
            // (if the db is hanging, we do want to force a cancel on the
            // connection)
            io.sleep(.fromSeconds(10), .real);
            _ = try future.cancel();
            return error.Canceled;
        },
    };
    _ = results; //write response
}
