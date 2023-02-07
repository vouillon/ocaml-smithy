module StringMap = Map.Make (struct
  type t = string

  let compare = compare
end)

module Timestamp = struct
  let to_date_time t = CalendarLib.Printer.Calendar.sprint "%FT%RZ" t
  let time_secfrac_re = Re.(compile (seq [ char '.'; rep (rg '0' '9') ]))

  let from_date_time t =
    CalendarLib.Printer.CalendarPrinter.from_fstring "%FT%TZ"
      (Re.replace_string time_secfrac_re ~by:"" t)

  let to_epoch_seconds = CalendarLib.Calendar.to_unixfloat
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

  let list ?(name = "member") f ?flat x =
    let name = Option.value ~default:name flat in
    List.map (fun y -> `Elt (name, f y)) x

  let map ?(key_name = "key") ?(value_name = "value") f ?flat x =
    let entry_name = Option.value ~default:"entry" flat in
    List.map
      (fun (k, v) ->
        `Elt
          (entry_name, [ `Elt (key_name, [ `Data k ]); `Elt (value_name, f v) ]))
      (StringMap.bindings x)

  let field name f x = [ `Elt (name, f x) ]
  let structure l = List.concat l
end

module From_XML = struct
  type t = [ `Data of string | `Elt of string * t ] list

  let string x = match x with [ `Data d ] -> d | [] -> "" | _ -> assert false

  let boolean x =
    match string x with "true" -> true | "false" -> false | _ -> assert false

  let blob x = Base64.decode_exn (string x)
  let integer x = Int32.of_string (string x)
  let long x = Int64.of_string (string x)

  let float x =
    match string x with
    | "NaN" -> 0. /. 0.
    | "Infinity" -> 1. /. 0.
    | "-Infinity" -> -1. /. 0.
    | x -> float_of_string x

  let timestamp x = Timestamp.from_date_time (string x)
  let document _ = assert false

  let select_field name x =
    List.filter
      (fun v -> match v with `Elt (name', _) -> name = name' | _ -> false)
      x

  let field name f x =
    match select_field name x with
    | [ `Elt (_, v) ] -> f v
    | _ -> assert false (*ZZZ*)

  let opt_field name f x =
    match select_field name x with
    | [ `Elt (_, v) ] -> Some (f v)
    | [] -> None
    | _ -> assert false (*ZZZ*)

  let flattened_field name (f : ?flat:_ -> _) x =
    f ~flat:true (select_field name x)

  let structure l =
    List.fold_left
      (fun m x ->
        match x with
        | `Data _ -> assert false
        | `Elt (k, v) ->
            StringMap.update k
              (fun l -> Some (v :: Option.value ~default:[] l))
              m)
      StringMap.empty l

  let list ?(name = "values") f ?(flat = false) x =
    List.map
      (fun v ->
        match v with
        | `Elt (nm, v) when flat || nm = name -> f v
        | _ -> assert false)
      x

  let map ?(key_name = "key") ?(value_name = "value") f ?(flat = false) x =
    List.fold_left
      (fun m v ->
        match v with
        | `Elt (nm, v) when flat || nm = "entry" ->
            StringMap.add (field key_name string v) (field value_name f v) m
        | _ -> assert false)
      StringMap.empty x

  let union x = match x with [ `Elt (k, v) ] -> (k, v) | _ -> assert false
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
