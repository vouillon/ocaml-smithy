module StringMap = Map.Make (struct
  type t = string

  let compare = compare
end)

module Timestamp = struct
  let to_date_time t =
    CalendarLib.Printer.Calendar.sprint "%FT%RZ" t (*ZZZ Check*)

  let to_epoch_seconds = CalendarLib.Calendar.to_unixfloat

  let from_date_time t =
    CalendarLib.Printer.CalendarPrinter.from_fstring "%FT%TZ" t

  let from_epoch_seconds = CalendarLib.Calendar.from_unixfloat
end

module To_JSON = struct
  let boolean x = `Bool x
  let blob x = `String (Base64.encode_string x)
  let string x = `String x
  let integer x = `Intlit (Int32.to_string x)
  let long x = `Intlit (Int64.to_string x)

  let float x =
    if x <> x then `String "NaN"
    else if 1. /. x <> 0. then `Float x
    else `String (if x < 0. then "Infinity" else "-Infinity")

  let timestamp x = `Float (Timestamp.to_epoch_seconds x)
  let document x = x
  let option f x = match x with None -> `Null | Some x -> f x
  let list f x = `List (List.map f x)
  let map f x = `Assoc (StringMap.bindings (StringMap.map f x))
  let structure l = `Assoc l
end

module From_JSON = struct
  open Yojson.Safe.Util

  let boolean = to_bool
  let blob x = Base64.decode_exn (to_string x)
  let string x = to_string x

  let integer x =
    match x with
    | `Int x -> Int32.of_int x
    | `Intlit x -> Int32.of_string x
    | _ -> assert false

  let long x =
    match x with
    | `Int x -> Int64.of_int x
    | `Intlit x -> Int64.of_string x
    | _ -> assert false

  let float x =
    match x with
    | `String "NaN" -> 0. /. 0.
    | `String "Infinity" -> 1. /. 0.
    | `String "-Infinity" -> -1. /. 0.
    | `Float x -> x
    | `Int x -> float x
    | `Intlit x -> float_of_string x
    | _ -> assert false

  let timestamp x = Timestamp.from_epoch_seconds (to_float x)
  let document x = x
  let option f x = if x = `Null then None else Some (f x)
  let list f x = List.map f (to_list x)

  let map f x =
    List.fold_left
      (fun m (k, v) -> StringMap.add k (f v) m)
      StringMap.empty (to_assoc x)

  let structure l = to_assoc l
  let unit _ = ()
end

(*
    ?ctx:ctx ->
    ?headers:Http.Header.t ->
    ?body:Body.t ->
    ?chunked:bool ->
    Http.Method.t ->
    Uri.t ->
    (Http.Response.t * Body.t) Lwt.t
*)

type meth = POST

type request = {
  uri : Uri.t;
  meth : meth;
  headers : (string * string) list;
  body : string option;
}

type response = { code : int; body : string }

type (+'perform, -'result, +'response, +'error) operation = {
  builder : Uri.t -> (request -> 'result) -> 'perform;
  parser : response -> ('response, 'error) Result.t;
}

type retryable = Retryable | Throttling | Non_retryable
type ('key, 'value) map = 'value StringMap.t

let create_JSON_operation ~variant ~host_prefix ~target ~builder ~parser ~errors
    =
  (* Use hostname, protocol (https if not set) and path from endpoint *)
  let headers =
    [
      ( "Content-Type",
        match variant with
        | `AwsJson1_0 -> "application/x-amz-json-1.0"
        | `AwsJson1_1 -> "application/x-amz-json-1.1" );
      ("X-Amz-Target", target);
    ]
  in
  {
    builder =
      (fun endpoint k ->
        builder (fun body ->
            (*ZZZ*)
            assert (Uri.host endpoint <> None);
            k
              {
                uri =
                  Uri.with_uri
                    ~host:
                      (Option.map
                         (fun host_prefix ->
                           host_prefix
                           ^ Uri.host_with_default ~default:"" endpoint)
                         host_prefix)
                    endpoint;
                meth = POST;
                headers;
                body = Some (Yojson.Safe.to_string body);
              }));
    parser =
      (fun response ->
        if response.code < 300 then
          Result.Ok (parser (Yojson.Safe.from_string response.body))
        else
          Result.error
            (List.assoc "foo" (*ZZZ*) errors
               (Yojson.Safe.from_string response.body)));
  }

module To_XML = struct
  type t = [ `Data of string | `Elt of string * t ] list

  let boolean x = [ `Data (if x then "true" else "false") ]
  let blob x = [ `Data (Base64.encode_string x) ]
  let string x = [ `Data x ]
  let integer x = [ `Data (Int32.to_string x) ]
  let long x = [ `Data (Int64.to_string x) ]

  let float x =
    [
      `Data
        (if x <> x then "NaN"
        else if 1. /. x <> 0. then Float.to_string x (*ZZZ*)
        else if x < 0. then "Infinity"
        else "-Infinity");
    ]

  let timestamp x = [ `Data (Timestamp.to_date_time x) ]
  let document _ = assert false
  let option f x = match x with None -> [] | Some x -> f x

  let list ?(name = "values") f ?flat x =
    let name = Option.value ~default:name flat in
    let l = List.map (fun y -> `Elt (name, f y)) x in
    if flat <> None then l else [ `Elt ("values", l) ]

  let map ?(key_name = "key") ?(value_name = "value") f ?flat x =
    let entry_name = Option.value ~default:"entry" flat in
    let values =
      List.map
        (fun (k, v) ->
          `Elt
            ( entry_name,
              [ `Elt (key_name, [ `Data k ]); `Elt (value_name, f v) ] ))
        (StringMap.bindings x)
    in
    if flat <> None then values else [ `Elt ("values", values) ]

  let field name f x = [ `Elt ("name", f x) ]
  let structure l = List.concat l
end

module From_XML = struct
  let string i =
    match Xmlm.peek i with
    | `Data d ->
        ignore (Xmlm.input i);
        d
    | `El_end -> ""
    | _ -> assert false

  let boolean i =
    match string i with "true" -> true | "false" -> false | _ -> assert false

  let blob i = Base64.decode_exn (string i)
  let integer i = Int32.of_string (string i)
  let long i = Int64.of_string (string i)

  let float i =
    match string i with
    | "NaN" -> 0. /. 0.
    | "Infinity" -> 1. /. 0.
    | "-Infinity" -> -1. /. 0.
    | x -> float_of_string x

  let timestamp i = Timestamp.from_date_time (string i)
  let document _ = assert false

  open Yojson.Safe.Util

  let option f x = if x = `Null then None else Some (f x)
  let list f x = List.map f (to_list x)

  let map f x =
    List.fold_left
      (fun m (k, v) -> StringMap.add k (f v) m)
      StringMap.empty (to_assoc x)

  let structure l = to_assoc l
  let unit _ = ()
end

module To_Graph = struct
  type t = [ `Data of string | `Elt of string * t ] list

  let boolean x = [ `Data (if x then "true" else "false") ]
  let blob x = [ `Data (Base64.encode_string x) ]
  let string x = [ `Data x ]
  let integer x = [ `Data (Int32.to_string x) ]
  let long x = [ `Data (Int64.to_string x) ]

  let float x =
    [
      `Data
        (if x <> x then "NaN"
        else if 1. /. x <> 0. then Float.to_string x (*ZZZ*)
        else if x < 0. then "Infinity"
        else "-Infinity");
    ]

  let timestamp x = [ `Data (Timestamp.to_date_time x) ]
  let document _ = assert false
  let option f x = match x with None -> [] | Some x -> f x

  let list ?(name = "member") f ?(flat = false) x =
    let l = List.mapi (fun i y -> `Elt (string_of_int i, f y)) x in
    if flat then l else [ `Elt (name, l) ]

  let map ?(name = "entry") ?(key_name = "key") ?(value_name = "value") f
      ?(flat = false) x =
    let values =
      List.mapi
        (fun i (k, v) ->
          `Elt
            ( string_of_int i,
              [ `Elt (key_name, [ `Data k ]); `Elt (value_name, f v) ] ))
        (StringMap.bindings x)
    in
    if flat then values else [ `Elt (name, values) ]

  let field name f x = [ `Elt ("name", f x) ]
  let structure l = List.concat l
end
