open Lwt

open Camltc

let mode = Bdb.default_mode
let lcnum = 1024
let ncnum = 512
let value = String.make (512 + 128) 'b'

let replicate c f =
    let rec loop acc = function
      | 0 -> acc
      | n -> loop (f () :: acc) (n - 1)
    in
    loop [] c

let sequential f t keys =
    let rec loop = function
      | [] -> ()
      | (x :: xs) ->
            let _ = f x in
            loop xs
    in
    let n_times g = 
        let rec go = function
          | 0 -> ()
          | n -> let () = g () in go (n - 1)
        in
        go
    in
    n_times (fun () -> loop keys) t

let concurrent f t keys =
    let rec loop = function
      | [] -> Lwt.return ()
      | (x :: xs) ->
            let _ = f x in
            loop xs
    in
    let ts = replicate t (fun () -> loop keys) in
    Lwt.join ts

let threaded f t keys =
    let rec loop = function
      | [] -> ()
      | (x :: xs) ->
            let _ = f x in
            loop xs
    in
    let ts = replicate t (fun () -> Lwt_preemptive.detach loop keys) in
    Lwt.join ts

let preemptive f t keys =
    let rec loop = function
      | [] -> Lwt.return ()
      | (x :: xs) ->
            Lwt_preemptive.detach (fun () -> f x) () >>= fun _ ->
            loop xs
    in
    let ts = replicate t (fun () -> loop keys) in
    Lwt.join ts


let make_db () =
    let fn = Printf.sprintf "camltc_concurrent-%d.db" (Unix.getpid ()) in
    Hotc.create ~mode ~lcnum ~ncnum fn [Bdb.BDBTLARGE] >>= fun db ->
    let bdb = Hotc.get_bdb db in
    Lwt.return (bdb, fn)

let make_keys =
    let suffix = String.create 256 in
    let rec loop acc = function
      | 0 -> acc
      | n ->
            let k = Printf.sprintf "%d%s" n suffix in
            loop (k :: acc) (n - 1)
    in
    loop []

let fill_db db = List.iter (fun k -> Bdb.put db k value)

let time n f =
    (* Warm up *)
    f () >>= fun () ->
    let s = Unix.gettimeofday () in
    f () >>= fun () ->
    let e = Unix.gettimeofday () in
    let l = e -. s in
    Lwt_io.printlf "Time: '%s' took %fs" n l

let main () =
    Lwt_preemptive.set_bounds (6, 6);

    let cnt = 100000 in
    Lwt_io.printlf "Creating key list, this might take a while" >>= fun () ->
    Lwt_io.flush_all () >>= fun () ->
    let keys = make_keys cnt in
    Lwt_io.printlf "Using %d keys" cnt >>= fun () ->

    make_db () >>= fun (db, fn) ->
    fill_db db keys;

    time "sequential 6"
        (fun () -> sequential (Bdb.get db) 6 keys; Lwt.return ()) >>= fun () ->
    time "sequential nolock 6"
        (fun () -> sequential (Bdb.get_nolock db) 6 keys; Lwt.return ()) >>= fun () ->
    time "concurrent 6"
        (fun () -> concurrent (Bdb.get db) 6 keys) >>= fun () ->
    time "concurrent nolock 6"
        (fun () -> concurrent (Bdb.get_nolock db) 6 keys) >>= fun () ->
    time "threaded 6"
        (fun () -> threaded (Bdb.get db) 6 keys) >>= fun () ->
    time "threaded nolock 6"
        (fun () -> threaded (Bdb.get_nolock db) 6 keys) >>= fun () ->
    time "preemptive 6"
        (fun () -> preemptive (Bdb.get db) 6 keys) >>= fun () ->
    time "preemptive nolock 6"
        (fun () -> preemptive (Bdb.get_nolock db) 6 keys) >>= fun () ->

    Lwt_unix.unlink fn
;;

Lwt_main.run (main ())
