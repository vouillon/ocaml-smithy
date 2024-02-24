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

module To_String = struct
  let boolean x = if x then "true" else "false"
  let integer = Int.to_string
  let long = Int64.to_string

  let byte x =
    Int.to_string
      (let c = Char.code x in
       if c > 127 then c - 256 else c)

  let float x =
    if x <> x then "NaN"
    else if 1. /. x <> 0. then Float.to_string x
    else if x < 0. then "-Infinity"
    else "Infinity"

  let timestamp x = Printf.sprintf "%.0f" (Timestamp.to_epoch_seconds x)
end

module To_JSON = struct
  let boolean x = `Bool x
  let blob x = `String (Base64.encode_string x)
  let string x = `String x
  let integer x = `Int x
  let long x = `Intlit (Int64.to_string x)

  let byte x =
    `Int
      (let c = Char.code x in
       if c > 127 then c - 256 else c)

  let float x =
    if x <> x then `String "NaN"
    else if 1. /. x <> 0. then `Float x
    else `String (if x < 0. then "-Infinity" else "Infinity")

  let timestamp x =
    `Intlit (Printf.sprintf "%.0f" (Timestamp.to_epoch_seconds x))

  let document x = x
  let option f x = match x with None -> `Null | Some x -> f x
  let list f x = `List (List.map f x)
  let map f x = `Assoc (StringMap.bindings (StringMap.map f x))
  let structure l = `Assoc (List.filter (fun (k, v) -> v <> `Null) l)
end

module From_JSON = struct
  open Yojson.Safe.Util

  let boolean = to_bool
  let blob x = Base64.decode_exn (to_string x)
  let string x = to_string x

  let integer x =
    match x with
    | `Int x -> x
    | `Intlit x -> int_of_string x
    | _ -> assert false

  let long x =
    match x with
    | `Int x -> Int64.of_int x
    | `Intlit x -> Int64.of_string x
    | _ -> assert false

  let byte x = Char.chr (integer x land 255)

  let float x =
    match x with
    | `String "NaN" -> 0. /. 0.
    | `String "Infinity" -> 1. /. 0.
    | `String "-Infinity" -> -1. /. 0.
    | `Float x -> x
    | `Int x -> float x
    | `Intlit x -> float_of_string x
    | _ -> assert false

  let timestamp x = Timestamp.from_epoch_seconds (to_number x)
  let document x = x
  let option f x = if x = `Null then None else Some (f x)
  let list f x = List.map f (to_list x)

  let map f x =
    List.fold_left
      (fun m (k, v) -> StringMap.add k (f v) m)
      StringMap.empty (to_assoc x)

  let structure l = to_assoc l
  let field nm l = try List.assoc nm l with Not_found -> `Null
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

type meth = GET | POST | PUT | HEAD | DELETE | PATCH

type request = {
  uri : Uri.t;
  meth : meth;
  headers : (string * string) list;
  body : string option;
}

type response = { code : int; headers : (string * string) list; body : string }

type (+'perform, -'result, +'response, +'error) operation = {
  builder : test:bool -> Uri.t -> (request -> 'result) -> 'perform;
  parser : response -> ('response, 'error) Result.t;
}

type retryable = Retryable | Throttling | Non_retryable
type ('key, 'value) map = 'value StringMap.t

let create_JSON_operation ~variant ~target ~builder ~parser ~errors =
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
      (fun ~test endpoint k ->
        builder ~test (fun body host_prefix ->
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
        let body =
          if response.body = "" then `Assoc []
          else Yojson.Safe.from_string response.body
        in
        if response.code < 300 then Result.Ok (parser body)
        else Result.error (List.assoc "foo" (*ZZZ*) errors body));
  }

let create_rest_json_operation ~method_ ~builder ~parser ~errors =
  (* Use hostname, protocol (https if not set) and path from endpoint *)
  let headers = [ ("Content-Type", "application/json") ] in
  {
    builder =
      (fun ~test endpoint k ->
        builder ~test (fun body host_prefix uri query _headers ->
            (*ZZZ*)
            let uri = Uri.of_string uri in
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
                    ~path:
                      (Some
                         (let p = Uri.path endpoint in
                          (if String.ends_with ~suffix:"/" p then
                           String.sub p 0 (String.length p - 1)
                          else p)
                          ^ Uri.path uri))
                    ~query:(Some (Uri.query uri @ query))
                    endpoint;
                meth = method_;
                headers = (match body with `Assoc [] -> [] | _ -> headers);
                body =
                  (match body with
                  | `Assoc [] -> None
                  | _ -> Some (Yojson.Safe.to_string body));
              }));
    parser =
      (fun response ->
        let body =
          if response.body = "" then `Assoc []
          else Yojson.Safe.from_string response.body
        in
        if response.code < 300 then Result.Ok (parser body)
        else Result.error (List.assoc "foo" (*ZZZ*) errors body));
  }

module To_XML = struct
  type t = [ `Data of string | `Elt of string * t ] list

  let boolean x = [ `Data (if x then "true" else "false") ]
  let blob x = [ `Data (Base64.encode_string x) ]
  let string x = [ `Data x ]
  let integer x = [ `Data (Int.to_string x) ]
  let long x = [ `Data (Int64.to_string x) ]

  let byte x =
    [
      `Data
        (Int.to_string
           (let c = Char.code x in
            if c > 127 then c - 256 else c));
    ]

  let float x =
    [
      `Data
        (if x <> x then "NaN"
        else if 1. /. x <> 0. then Float.to_string x (*ZZZ*)
        else if x < 0. then "-Infinity"
        else "Infinity");
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
  let integer x = int_of_string (string x)
  let long x = Int64.of_string (string x)
  let byte x = Char.chr (int_of_string (string x) land 255)

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
  let integer x = [ `Data (Int.to_string x) ]
  let long x = [ `Data (Int64.to_string x) ]

  let float x =
    [
      `Data
        (if x <> x then "NaN"
        else if 1. /. x <> 0. then Float.to_string x (*ZZZ*)
        else if x < 0. then "-Infinity"
        else "Infinity");
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

  let field name f x = [ `Elt (name, f x) ]
  let structure l = List.concat l
end

(*ZZZ This should be generated *)
module Endpoint = struct
  type region_info = {
    supports_fips : bool;
    supports_dual_stack : bool;
    name : string;
    dns_suffix : string;
    dual_stack_dns_suffix : string;
  }

  let partitions =
    let region_suffix_re =
      Re.(
        seq
          [
            char '-';
            rep1 (alt [ rg 'a' 'z'; rg 'A' 'Z'; rg '0' '9' ]);
            char '-';
            rep1 (rg '0' '9');
          ])
    in
    [
      ( {
          dns_suffix = "amazonaws.com";
          dual_stack_dns_suffix = "api.aws";
          name = "aws";
          supports_dual_stack = true;
          supports_fips = true;
        },
        Re.(
          compile
            (whole_string
               (seq
                  [
                    alt
                      (List.map str
                         [ "us"; "eu"; "ap"; "sa"; "ca"; "me"; "af" ]);
                    region_suffix_re;
                  ]))),
        [
          "af-south-1";
          "ap-east-1";
          "ap-northeast-1";
          "ap-northeast-2";
          "ap-northeast-3";
          "ap-south-1";
          "ap-south-2";
          "ap-southeast-1";
          "ap-southeast-2";
          "ap-southeast-3";
          "ap-southeast-4";
          "aws-global";
          "ca-central-1";
          "eu-central-1";
          "eu-central-2";
          "eu-north-1";
          "eu-south-1";
          "eu-south-2";
          "eu-west-1";
          "eu-west-2";
          "eu-west-3";
          "me-central-1";
          "me-south-1";
          "sa-east-1";
          "us-east-1";
          "us-east-2";
          "us-west-1";
          "us-west-2";
        ] );
      ( {
          dns_suffix = "amazonaws.com.cn";
          dual_stack_dns_suffix = "api.amazonwebservices.com.cn";
          name = "aws-cn";
          supports_dual_stack = true;
          supports_fips = true;
        },
        Re.(
          compile
            (whole_string
               (seq
                  [
                    str "cn-";
                    rep1 (alt [ rg 'a' 'z'; rg 'A' 'Z'; rg '0' '9' ]);
                    char '-';
                    rep1 (rg '0' '9');
                  ]))),
        [ "aws-cn-global"; "cn-north-1"; "cn-northwest-1" ] );
      ( {
          dns_suffix = "amazonaws.com";
          dual_stack_dns_suffix = "api.aws";
          name = "aws-us-gov";
          supports_dual_stack = true;
          supports_fips = true;
        },
        Re.(compile (whole_string (seq [ str "us-gov"; region_suffix_re ]))),
        [ "aws-us-gov-global"; "us-gov-east-1"; "us-gov-west-1" ] );
      ( {
          dns_suffix = "c2s.ic.gov";
          dual_stack_dns_suffix = "c2s.ic.gov";
          name = "aws-iso";
          supports_dual_stack = false;
          supports_fips = true;
        },
        Re.(compile (whole_string (seq [ str "us-iso"; region_suffix_re ]))),
        [ "aws-iso-global"; "us-iso-east-1"; "us-iso-west-1" ] );
      ( {
          dns_suffix = "sc2s.sgov.gov";
          dual_stack_dns_suffix = "sc2s.sgov.gov";
          name = "aws-iso-b";
          supports_dual_stack = false;
          supports_fips = true;
        },
        Re.(compile (whole_string (seq [ str "us-isob"; region_suffix_re ]))),
        [ "aws-iso-b-global"; "us-isob-east-1" ] );
    ]

  let partition r : region_info option =
    let output, _, _ =
      try List.find (fun (_, _, lst) -> List.mem r lst) partitions
      with Not_found -> (
        try List.find (fun (_, re, _) -> Re.execp re r) partitions
        with Not_found ->
          List.find (fun (output, _, _) -> output.name = "aws") partitions)
    in
    Some output

  let valid_host_label_re =
    Re.(
      let alphanum = alt [ rg 'a' 'z'; rg 'A' 'Z'; rg '0' '9' ] in
      compile
        (whole_string
           (seq [ repn (alt [ alphanum; char '-' ]) 0 (Some 62); alphanum ])))

  let rec is_valid_host_label x allow_subdomains =
    if not allow_subdomains then Re.execp valid_host_label_re x
    else
      List.for_all
        (fun x -> is_valid_host_label x false)
        (String.split_on_char '.' x)

  type arn = {
    partition : string;
    service : string;
    region : string;
    account_id : string;
    resource_id : string array;
  }

  let parse_arn arn =
    let segments = String.split_on_char ':' arn in
    match segments with
    | "arn" :: partition :: service :: region :: account_id
      :: (r :: _ as resource_id) ->
        if partition = "" || service = "" || r = "" then None
        else
          Some
            {
              partition;
              service;
              region;
              account_id;
              resource_id = Array.of_list resource_id;
            }
    | _ -> None

  let rec is_virtual_hostable_s3_bucket x allow_subdomains =
    if allow_subdomains then
      List.for_all
        (fun x -> is_virtual_hostable_s3_bucket x false)
        (String.split_on_char '.' x)
    else
      is_valid_host_label x false
      && String.length x >= 3
      && x = String.lowercase_ascii x

  type url = {
    scheme : string;
    is_ip : bool;
    authority : string;
    path : string;
    normalized_path : string;
  }

  let ip_address_re =
    let open Re in
    let digit = rg '0' '9' in
    let byte =
      alt
        [
          seq [ char '2'; char '5'; rg '0' '5' ];
          seq [ char '2'; rg '0' '4'; digit ];
          seq [ char '1'; digit; digit ];
          seq [ rg '1' '9'; digit ];
          digit;
        ]
    in
    Re.compile
      (whole_string
         (alt
            [
              seq [ byte; char '.'; byte; char '.'; byte; char '.'; byte ];
              seq [ char '['; rep any; char ']' ];
            ]))

  let is_ip_address s = Re.execp ip_address_re s

  let parse_url uri : url option =
    Option.bind (Uri.scheme uri) @@ fun scheme ->
    Option.bind (Uri.host uri) @@ fun host ->
    let path = Uri.path uri in
    let authority =
      match Uri.port uri with
      | Some port -> host ^ ":" ^ string_of_int port
      | None -> host
    in
    Some
      {
        scheme;
        is_ip = is_ip_address host;
        authority;
        path;
        normalized_path =
          (if String.ends_with ~suffix:"/" path then path else path ^ "/");
      }

  let array_get_opt a i = if i >= Array.length a then None else Some a.(i)

  let substring s i j rev =
    let l = String.length s in
    if j > l then None
    else
      Some
        (if rev then String.sub s (l - j) (j - i) else String.sub s i (j - i))

  module Rules = struct
    let ( let* ) = Option.bind
    let check b = if b then Some () else None

    let rec seq l =
      match l with
      | [] -> assert false
      | f :: rem -> ( match f () with Some x -> Some x | None -> seq rem)
  end
end
