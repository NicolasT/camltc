open Core.Std
open Core_bench.Std

open Lwt

open Camltc

let db_file = Printf.sprintf "camltc_bench-%d.db" (Unix.getpid () |> Pid.to_int)
let lcnum = 1024
let ncnum = 512
let mode = Bdb.default_mode

let sizes = [17; 65; 257; 1024; 1025; 2049; 4097]
let make_string i = String.make i 'a'

let make_db () =
  Hotc.create db_file ~lcnum ~ncnum ~mode [Bdb.BDBTLARGE] >>= fun db ->
  Lwt.return (Hotc.get_bdb db)


let fill_db db =
  List.iter ~f:(fun l ->
      let k = make_string l in
      Bdb.put db k "value")
    sizes

let make_bench db =
  fill_db db;
  Bench.make_command [
    Bench.Test.create_indexed
      ~name:"bdb_get"
      ~args:sizes
      (fun l ->
        let k = make_string l in
        Staged.stage (fun () -> ignore (Bdb.get db k)));
    Bench.Test.create_indexed
      ~name:"bdb_get_nolock"
      ~args:sizes
      (fun l ->
        let k = make_string l in
        Staged.stage (fun () -> ignore (Bdb.get_nolock db k)));
  ]


let main () =
  let exists =
    try
      ignore (Unix.stat db_file);
      true
    with Unix.Unix_error (Unix.ENOENT, _, _) -> false
  in
  if exists
    then
      let msg = Printf.sprintf "Database file %s exists" db_file in
      failwith msg
    else
      ();

  let db = Lwt_main.run (make_db ()) in
  Command.run (make_bench db);
  Unix.unlink db_file
;;

main ()
