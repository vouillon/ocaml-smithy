module StringMap = Map.Make (struct
  type t = string

  let compare = compare
end)

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

  let timestamp x = `Float (CalendarLib.Calendar.to_unixfloat x)
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

  let timestamp x = CalendarLib.Calendar.from_unixfloat (to_float x)
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
        if response.code = 200 then
          Result.Ok (parser (Yojson.Safe.from_string response.body))
        else
          Result.error
            (List.assoc "foo" (*ZZZ*) errors
               (Yojson.Safe.from_string response.body)));
  }
