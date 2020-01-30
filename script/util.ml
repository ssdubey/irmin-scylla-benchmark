let rec func kvlist line_sep =
let kvpairlist = (match line_sep with
| p::p_lst -> (let pair = String.split_on_char(',') p in
        let a = (match pair with | k::v::[] -> (k,v) | _ -> ("","")) in
          func (a::kvlist) p_lst)
| _ -> kvlist) in
print_string ("\nkvpairlist length in rec func: " ); print_int (List.length kvpairlist);
kvpairlist

let benchmark inputbuf =
  let line_sep = String.split_on_char('\n') inputbuf in
  print_string ("\nline sep length " ); print_int (List.length line_sep);
  let kvpairlist = func [] line_sep in
  kvpairlist

let rec fprint kvpairlist = 
match kvpairlist with
|h::t -> let k, v = h in
        print_string ("\nkey =" ^ k); print_string ("value =" ^ v) ;
        fprint t
| _ -> ()

let readfile fileloc = 
let buf = Buffer.create 4096 in
try
    while true do
      let line = input_line fileloc in
      Buffer.add_string buf line;
      Buffer.add_char buf '\n'
    done;
    assert false (* This is never executed
                    (always raise Assert_failure). *)
  with
    End_of_file -> Buffer.contents buf

(* let _ = *)
let kvpairfun () = 
  let contentbuf = readfile (open_in "/home/shashank/work/benchmark_irminscylla/input/inpfile") in 
  let kvpairlist = benchmark contentbuf in
(* print_string ("\nkvpairlist length in main:" );print_int (List.length kvpairlist);
fprint kvpairlist *)
kvpairlist

