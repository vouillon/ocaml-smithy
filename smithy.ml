(*
TODO
- generate operations for all protocols
- sign and perform requests

- mli files

- timestampFormat

- validation: length, pattern, range, uniqueItems  (put that in the documentation?)

      4 aws.protocols#restXml     <== S3
     17 aws.protocols#awsQuery
     23 aws.protocols#awsJson1_0
    106 aws.protocols#awsJson1_1
    186 aws.protocols#restJson1

./gradlew :smithy-aws-protocol-tests:build

Things to consider
- endpoint configuration
- streaming
- pagination ==> modification of the request / access to the response
- waiters
- retries ==> retryable errors / idempotency
- presigned URLs

Compiling an operation:
- builder function (straight for arguments to JSon)
  ==> json + host prefix + uri ?

To_XML ==> xmlNamespace / xmlAttribute

type 'a error = {code : int; name : string; body: string; value : 'a }



context :
==> Lwt/async
==> retry policy
==> endpoint configuration
==> pagination

*)

open Yojson.Safe

module StringMap = Map.Make (struct
  type t = string

  let compare = compare
end)

type shape_id = { namespace : string; identifier : string }

let parse_shape_id s =
  let i = String.index s '#' in
  {
    namespace = String.sub s 0 i;
    identifier = String.sub s (i + 1) (String.length s - i - 1);
  }

module IdMap = Map.Make (struct
  type t = shape_id

  let compare = compare
end)

module IdSet = Set.Make (struct
  type t = shape_id

  let compare = compare
end)

type traits = (string * Yojson.Safe.t) list

type http_request_test = {
  id : string;
  documentation : string option;
  method_ : string option;
  uri : string;
  query_params : string list option;
  forbid_query_params : string list;
  require_query_params : string list;
  headers : (string * string) list option;
  forbid_headers : string list;
  require_headers : string list;
  body : string option;
  body_media_type : string option;
  host : string option;
  resolved_host : string option;
  params : Yojson.Safe.t;
  applies_to : [ `Client | `Server ] option;
}

type http_response_test = {
  id : string;
  documentation : string option;
  code : int;
  headers : (string * string) list option;
  body : string option;
  params : Yojson.Safe.t;
  applies_to : [ `Client | `Server ] option;
}

type shape =
  | Blob (* string *)
  | Boolean
  | String (* string *)
  | Enum of (string * shape_id * traits) list
  | Integer (* int32 *)
  | Long (* int64 *)
  | Float
  | Double
  | IntEnum
  | Short
  | Byte
  | Timestamp
  | Document
  | List of (shape_id * traits)
  | Map of (shape_id * traits) * (shape_id * traits)
  | Structure of (string * shape_id * traits) list
  | Union of (string * shape_id * traits) list
  | Service of service
  | Resource of resource
  | Operation of operation

and service = {
  version : string;
  operations : shape_id list;
  resources : shape_id list;
  errors : shape_id list;
  rename : string IdMap.t;
}

and resource = {
  create : shape_id option;
  put : shape_id option;
  read : shape_id option;
  update : shape_id option;
  delete : shape_id option;
  list : shape_id option;
  operations : shape_id list;
  collection_operations : shape_id list;
  resources : shape_id list;
}

and operation = {
  input : shape_id;
  output : shape_id;
  errors : shape_id list;
  http_request_tests : http_request_test list;
  http_response_tests : http_response_test list;
}

let unit_type = { namespace = "smithy.api"; identifier = "Unit" }

let parse_http_request_test test =
  let open Util in
  {
    id = test |> member "id" |> to_string;
    documentation = test |> member "documentation" |> to_option to_string;
    method_ = test |> member "method" |> to_option to_string;
    uri = test |> member "uri" |> to_string;
    host = test |> member "host" |> to_option to_string;
    resolved_host = test |> member "resolvedHost" |> to_option to_string;
    params = test |> member "params";
    query_params =
      test |> member "queryParams"
      |> to_option (fun h -> h |> to_list |> List.map to_string);
    forbid_query_params =
      test |> member "forbidQueryParams" |> to_option to_list
      |> Option.value ~default:[] |> List.map to_string;
    require_query_params =
      test
      |> member "requireQueryParams"
      |> to_option to_list |> Option.value ~default:[] |> List.map to_string;
    headers =
      test |> member "headers"
      |> to_option (fun h ->
             h |> to_assoc |> List.map (fun (k, v) -> (k, to_string v)));
    forbid_headers =
      test |> member "forbidHeaders" |> to_option to_list
      |> Option.value ~default:[] |> List.map to_string;
    require_headers =
      test |> member "requireHeaders" |> to_option to_list
      |> Option.value ~default:[] |> List.map to_string;
    body = test |> member "body" |> to_option to_string;
    body_media_type = test |> member "bodyMediaType" |> to_option to_string;
    applies_to =
      ( test |> member "appliesTo" |> fun s ->
        match to_option to_string s with
        | None -> None
        | Some "client" -> Some `Client
        | Some "server" -> Some `Server
        | _ -> assert false );
  }

let parse_http_response_test test =
  let open Util in
  {
    id = test |> member "id" |> to_string;
    documentation = test |> member "documentation" |> to_option to_string;
    code = test |> member "code" |> to_int;
    params = test |> member "params";
    headers =
      test |> member "headers"
      |> to_option (fun h ->
             h |> to_assoc |> List.map (fun (k, v) -> (k, to_string v)));
    body = test |> member "body" |> to_option to_string;
    applies_to =
      ( test |> member "appliesTo" |> fun s ->
        match to_option to_string s with
        | None -> None
        | Some "client" -> Some `Client
        | Some "server" -> Some `Server
        | _ -> assert false );
  }

let parse_shape (id, sh) =
  let typ = Util.(sh |> member "type" |> to_string) in
  let parse_traits m =
    Util.(
      m |> member "traits" |> to_option to_assoc |> Option.value ~default:[])
  in
  let parse_member name =
    let m = Util.(sh |> member name) in
    (Util.(m |> member "target" |> to_string |> parse_shape_id), parse_traits m)
  in
  let parse_members () =
    Util.(
      sh |> member "members" |> to_assoc
      |> List.map (fun (nm, m) ->
             let ty =
               Util.(m |> member "target" |> to_string |> parse_shape_id)
             in

             (nm, ty, parse_traits m)))
  in
  let parse_list name =
    Util.(
      sh |> member name |> to_option to_list |> Option.value ~default:[]
      |> List.map (fun m ->
             Util.(m |> member "target" |> to_string |> parse_shape_id)))
  in
  let traits = parse_traits sh in
  ( parse_shape_id id,
    ( (match typ with
      | "blob" -> Blob
      | "boolean" -> Boolean
      | "string" -> (
          try
            Enum
              (List.assoc "smithy.api#enum" traits
              |> Util.to_list
              |> List.map (fun e ->
                     let value =
                       Util.(
                         e |> member "name" |> to_option to_string
                         |> Option.value
                              ~default:(e |> member "value" |> to_string))
                     in
                     if String.contains value ' ' || String.contains value ':'
                     then raise Not_found;
                     (value, unit_type, [])))
          with Not_found -> String (* string *))
      | "enum" -> Enum (parse_members ())
      | "integer" -> Integer (* int32 *)
      | "long" -> Long (* int64 *)
      | "float" -> Float
      | "double" -> Double
      | "timestamp" -> Timestamp
      | "document" -> Document (* ??? *)
      | "list" -> List (parse_member "member")
      | "map" -> Map (parse_member "key", parse_member "value")
      | "structure" -> Structure (parse_members ())
      | "union" -> Union (parse_members ())
      | "service" ->
          Service
            {
              version = Util.(sh |> member "version" |> to_string);
              operations = parse_list "operations";
              resources = parse_list "resources";
              errors = parse_list "errors";
              rename =
                Util.(
                  sh |> member "rename" |> to_option to_assoc
                  |> Option.value ~default:[]
                  |> List.fold_left
                       (fun m (k, v) ->
                         IdMap.add (parse_shape_id k) (to_string v) m)
                       IdMap.empty);
            }
      | "resource" ->
          let parse_member_opt name =
            Util.(
              sh |> member name
              |> to_option (fun t ->
                     t |> member "target" |> to_string |> parse_shape_id))
          in
          Resource
            {
              create = parse_member_opt "create";
              put = parse_member_opt "put";
              read = parse_member_opt "read";
              update = parse_member_opt "update";
              delete = parse_member_opt "delete";
              list = parse_member_opt "list";
              operations = parse_list "operations";
              collection_operations = parse_list "collectionOperations";
              resources = parse_list "resources";
            }
      | "operation" ->
          Operation
            {
              input = fst (parse_member "input");
              output = fst (parse_member "output");
              errors = parse_list "errors";
              http_request_tests =
                Util.(
                  traits
                  |> List.assoc_opt "smithy.test#httpRequestTests"
                  |> Option.value ~default:`Null
                  |> to_option to_list |> Option.value ~default:[]
                  |> List.map parse_http_request_test);
              http_response_tests =
                Util.(
                  traits
                  |> List.assoc_opt "smithy.test#httpResponseTests"
                  |> Option.value ~default:`Null
                  |> to_option to_list |> Option.value ~default:[]
                  |> List.map parse_http_response_test);
            }
      | "intEnum" -> IntEnum
      | "short" -> Short
      | "byte" -> Byte
      | _ -> assert false),
      traits ) )

let parse f =
  let d = from_file f in
  let shapes = Util.(d |> member "shapes" |> to_assoc) in
  List.fold_left
    (fun map shape ->
      let id, shape = parse_shape shape in
      IdMap.add id shape map)
    IdMap.empty shapes

let to_snake_case =
  let uppercase = Re.rg 'A' 'Z' in
  let lowercase = Re.rg 'a' 'z' in
  let first_pattern_re =
    Re.(
      compile
        (seq [ group (rep1 uppercase); group (seq [ uppercase; lowercase ]) ]))
  in
  let second_pattern_re =
    Re.(
      compile (seq [ group (alt [ lowercase; rg '0' '9' ]); group uppercase ]))
  in
  let space_re = Re.(compile (set " -")) in
  let replace re s =
    Re.replace re ~f:(fun g -> Re.Group.get g 1 ^ "_" ^ Re.Group.get g 2) s
  in
  fun s ->
    s |> replace first_pattern_re |> replace second_pattern_re
    |> Re.replace space_re ~f:(fun _ -> "_")
    |> String.lowercase_ascii

let reserved_words =
  [
    "and";
    "begin";
    "constraint";
    "else";
    "end";
    "exception";
    "external";
    "function";
    "include";
    "match";
    "method";
    "module";
    "mutable";
    "object";
    "or";
    "then";
    "to";
    "type";
    "bool";
    "float";
    "int";
    "option";
    "string";
    "unit";
  ]

let uncapitalized_identifier s =
  let s = to_snake_case s in
  if List.mem s reserved_words then s ^ "_" else s

let all_upper_re =
  Re.(compile (whole_string (rep (alt [ rg 'A' 'Z'; rg '0' '9'; char '_' ]))))

let capitalized_identifier s =
  if Re.execp all_upper_re s then s
  else String.capitalize_ascii (to_snake_case s)

let type_name ~rename id =
  match id.namespace with
  | "smithy.api" -> (
      match id.identifier with
      | "Boolean" | "PrimitiveBoolean" -> "bool"
      | "Blob" | "String" -> "string"
      | "Integer" -> "int"
      | "Long" | "PrimitiveLong" -> "Int64.t"
      | "Float" | "Double" -> "float"
      | "Timestamp" -> "CalendarLib.Calendar.t"
      | "Document" -> "Yojson.Safe.t"
      | "Unit" -> "unit"
      | "Short" -> "int"
      | "Byte" -> "char"
      | _ -> assert false)
  | _ ->
      uncapitalized_identifier
        (try IdMap.find id rename with Not_found -> id.identifier)

let field_name = uncapitalized_identifier
let constr_name = capitalized_identifier

let optional_member (_, _, traits) =
  List.mem_assoc "smithy.api#clientOptional" traits
  || not
       (List.mem_assoc "smithy.api#required" traits
       || List.mem_assoc "smithy.api#default" traits)

let flattened_member (_, _, traits) =
  List.mem_assoc "smithy.api#xmlFlattened" traits

let type_of_shape shapes nm =
  if nm.namespace = "smithy.api" then (
    match nm.identifier with
    | "PrimitiveLong" -> Long
    | "Integer" -> Integer
    | "Long" -> Long
    | "String" -> String
    | "Float" -> Float
    | "Double" -> Double
    | "Boolean" | "PrimitiveBoolean" -> Boolean
    | "Short" -> Short
    | "Byte" -> Byte
    | "Blob" -> Blob
    | "Timestamp" -> Timestamp
    | _ ->
        Format.eprintf "%s/%s@." nm.namespace nm.identifier;
        assert false)
  else
    match IdMap.find_opt nm shapes with
    | None -> assert false
    | Some (typ, _) -> typ

let loc = Location.none

type html =
  | Text of string
  | Element of string * (string * string) list * html list

let rec text doc =
  match doc with
  | Text txt -> txt
  | Element (_, _, children) -> children_text children

and children_text ch = String.concat "" (List.map text ch)

let escape_code =
  let space_re = Re.(compile (rep1 (set " \n\t"))) in
  let escaped_re = Re.(compile (set "[]")) in
  let trailing_backslash_re = Re.(compile (seq [ char '\\'; stop ])) in
  fun txt ->
    txt
    |> Re.replace escaped_re ~f:(fun g -> "\\" ^ Re.Group.get g 0)
    |> Re.replace space_re ~f:(fun _ -> " ")
    |> Re.replace trailing_backslash_re ~f:(fun _ -> "\\ ")

let escape_text =
  let space_re = Re.(compile (rep1 (set " \n\t"))) in
  let escaped_re = Re.(compile (set "{[]}@")) in
  fun txt ->
    txt
    |> Re.replace escaped_re ~f:(fun g -> "\\" ^ Re.Group.get g 0)
    |> Re.replace space_re ~f:(fun _ -> " ")

let empty_text =
  let space_re = Re.(compile (whole_string (rep (set " \n\t")))) in
  fun txt -> Re.execp space_re txt

let rec fix_list l =
  match l with
  | Element ("li", attr, children) :: Text txt :: rem ->
      fix_list (Element ("li", attr, children @ [ Text txt ]) :: rem)
  | Element ("li", attr, children) :: (Element (nm, _, children') as elt) :: rem
    when nm <> "li" ->
      if nm = "b" then
        fix_list (Element ("li", attr, children) :: (children' @ rem))
      else fix_list (Element ("li", attr, children @ [ elt ]) :: rem)
  | elt :: rem -> elt :: fix_list rem
  | [] -> []

let rec format_dl ~format l dts (dds : _ list) =
  let format_group () =
    "{- "
    ^ String.concat " / "
        (List.rev_map
           (fun s -> "{b " ^ s ^ "}")
           (List.filter (fun s -> not (empty_text s)) dts))
    ^ " "
    ^ (match dds with
      | [ dd ] -> dd
      | _ ->
          "\n{ul "
          ^ String.concat " " (List.rev_map (fun s -> "{- " ^ s ^ "}") dds)
          ^ "}")
    ^ "}"
  in
  match l with
  | Element ("dt", [], children) :: rem ->
      if dds <> [] then
        format_group () ^ format_dl ~format rem (format children :: dts) []
      else format_dl ~format rem (format children :: dts) []
  | Element ("dd", [], children) :: rem ->
      format_dl ~format rem dts (format children :: dds)
  | Element _ :: _ -> assert false
  | Text txt :: rem ->
      assert (empty_text txt);
      format_dl ~format rem dts dds
  | [] -> if dts <> [] then format_group () else ""

let rec allowed_in_b l =
  List.for_all
    (fun item ->
      match item with
      | Element ("ul", _, _) -> false
      | Element (_, _, children) -> allowed_in_b children
      | Text _ -> true)
    l

let rec format ~shapes ~field_refs ~in_anchor ?(in_list = false) ~toplevel doc =
  match doc with
  | Text txt -> escape_text txt
  | Element ("p", _, children) ->
      let s =
        format_children ~shapes ~field_refs ~in_anchor ~toplevel:false children
      in
      if toplevel then s ^ "\n\n" else s
  | Element ("code", _, children) | Element ("a", [], children) -> (
      let s = escape_code (children_text children) in
      let reference =
        match StringMap.find_opt s field_refs with
        | Some typ ->
            Some
              (Printf.sprintf "{!type-%s.%s}"
                 (uncapitalized_identifier typ)
                 (uncapitalized_identifier s))
        | None -> (
            match
              IdMap.choose_opt
                (IdMap.filter
                   (fun { identifier; _ } _ -> identifier = s)
                   shapes)
            with
            | Some (_, (typ, _)) -> (
                match typ with
                | Service _ | Resource _ -> Some ("[" ^ s ^ "]")
                | Operation _ ->
                    Some ("[" ^ s ^ "]")
                    (*ZZZZ "{!val:" ^ uncapitalized_identifier s ^ "}" *)
                | _ -> Some ("{!type:" ^ uncapitalized_identifier s ^ "}"))
            | None -> None)
      in
      match reference with
      | Some reference when not in_anchor -> reference
      | _ -> "[" ^ s ^ "]")
  | Element (("i" | "replaceable" | "title"), _, children) ->
      let s =
        format_children ~shapes ~field_refs ~in_anchor ~toplevel:false children
      in
      if empty_text s then s else "{i " ^ s ^ "}"
  | Element ("b", _, children) ->
      let s =
        format_children ~shapes ~field_refs ~in_anchor ~toplevel:false children
      in
      if empty_text s || not (allowed_in_b children) then s else "{b " ^ s ^ "}"
  | Element (("note" | "para"), _, children) ->
      format_children ~shapes ~field_refs ~in_anchor ~toplevel children
  | Element ("important", _, children) ->
      format_children ~shapes ~field_refs ~in_anchor ~toplevel children
  | Element ("a", attr, children) when List.mem_assoc "href" attr ->
      let url = List.assoc "href" attr in
      let s =
        format_children ~shapes ~field_refs ~in_anchor:true ~toplevel children
      in
      if empty_text url then s else "{{: " ^ url ^ " }" ^ s ^ "}"
  | Element ("ul", _, children) ->
      "\n{ul "
      ^ format_children ~shapes ~field_refs ~in_anchor ~in_list:true
          ~toplevel:false (fix_list children)
      ^ "}\n"
  | Element ("li", _, children) ->
      let s =
        format_children ~shapes ~field_refs ~in_anchor ~toplevel:false children
      in
      if in_list then "{- " ^ s ^ "}" else s
  | Element ("dl", _, children) ->
      "\n{ul "
      ^ format_dl
          ~format:
            (format_children ~shapes ~field_refs ~in_anchor ~toplevel:false)
          children [] []
      ^ "}\n"
  | Element ("ol", _, children) ->
      "\n{ol "
      ^ format_children ~shapes ~field_refs ~in_anchor ~in_list:true
          ~toplevel:false (fix_list children)
      ^ "}\n"
  | Element ("br", _, []) -> "\n\n"
  | Element ("fullname", _, _) -> ""
  | Element (nm, _, children) ->
      let s =
        "<" ^ nm ^ ">"
        ^ format_children ~shapes ~field_refs ~in_anchor ~toplevel:false
            children
      in
      (*      Format.eprintf "AAA %s@." s;*)
      s

and format_children ~shapes ~field_refs ~in_anchor ?(in_list = false) ~toplevel
    lst =
  String.concat ""
    (List.map (format ~shapes ~field_refs ~in_anchor ~in_list ~toplevel) lst)

let documentation ~shapes ~field_refs doc =
  let open Markup in
  if doc = "" then None
  else
    "<body>" ^ doc |> string |> parse_html |> signals
    |> tree
         ~text:(fun ss -> Text (String.concat "" ss))
         ~element:(fun (_, name) attr children ->
           Element
             ( name,
               List.map (fun ((_, name), value) -> (name, value)) attr,
               children ))
    |> fun doc' ->
    match doc' with
    | Some (Element ("body", _, children)) ->
        Some
          (format_children ~shapes ~field_refs ~in_anchor:false ~toplevel:true
             children)
    | _ -> assert false

module B = Ppxlib.Ast_builder.Make (struct
  let loc = Location.none
end)

let const_string s = Ppxlib.Parsetree.Pconst_string (s, Location.none, None)
let doc_loc = Location.mknoloc "ocaml.doc"

let doc_attr doc =
  let exp = B.pexp_constant (const_string doc) in
  let item = B.pstr_eval exp [] in
  {
    Ppxlib.Parsetree.attr_name = doc_loc;
    attr_payload = PStr [ item ];
    attr_loc = loc;
  }

let text_loc = Location.mknoloc "ocaml.text"

let text_attr doc =
  let exp = B.pexp_constant (const_string doc) in
  let item = B.pstr_eval exp [] in
  {
    Ppxlib.Parsetree.attr_name = text_loc;
    attr_payload = PStr [ item ];
    attr_loc = loc;
  }

let documentation ~shapes ?(field_refs = StringMap.empty) traits =
  match List.assoc_opt "smithy.api#documentation" traits with
  | None -> []
  | Some doc -> (
      let doc = Yojson.Safe.Util.to_string doc in
      match documentation ~shapes ~field_refs doc with
      | None | Some "" -> []
      | Some doc -> [ doc_attr doc ])

let toplevel_documentation ~shapes traits =
  List.map
    (fun d -> B.pstr_attribute { d with attr_name = text_loc })
    (documentation ~shapes traits)

let type_ident ~rename id =
  B.ptyp_constr (Location.mknoloc (Longident.Lident (type_name ~rename id))) []

let member_name ~fixed ?(name = "smithy.api#jsonName") (nm, _, traits) =
  if fixed then nm
  else
    try Yojson.Safe.Util.to_string (List.assoc name traits)
    with Not_found -> nm

let default_value ~shapes ~rename typ default =
  match default with
  | `String s -> (
      match type_of_shape shapes typ with
      | String | Blob -> B.pexp_constant (const_string s)
      | Enum l ->
          let nm, _, _ =
            List.find
              (fun enum ->
                member_name ~fixed:false ~name:"smithy.api#enumValue" enum = s)
              l
          in
          [%expr
            ([%e
               B.pexp_construct
                 (Location.mknoloc (Longident.Lident (constr_name nm)))
                 None]
              : [%t type_ident ~rename typ])]
      | _ ->
          prerr_endline typ.identifier;
          assert false)
  | `Int n ->
      B.pexp_constant
        (match type_of_shape shapes typ with
        | Float | Double -> Pconst_float (Printf.sprintf "%d." n, None)
        | Integer -> Pconst_integer (Int.to_string n, None)
        | Long -> Pconst_integer (Int.to_string n, Some 'L')
        | _ -> assert false)
  | `Bool b -> if b then [%expr true] else [%expr false]
  | `List [] -> [%expr []]
  | _ ->
      Format.eprintf "DEFAULT %s@." (Yojson.Safe.to_string default);
      assert false

let structure_has_optionals fields =
  List.exists
    (fun (_, _, traits) ->
      List.mem_assoc "smithy.api#default" traits
      || List.mem_assoc "smithy.api#clientOptional" traits
      || not (List.mem_assoc "smithy.api#required" traits))
    fields

let constructor_parameters ~shapes ~rename ~fields ~body =
  let has_optionals = structure_has_optionals fields in
  List.fold_right
    (fun field expr ->
      let nm, typ, traits' = field in
      match List.assoc_opt "smithy.api#default" traits' with
      | Some default when default <> `Null ->
          let default = default_value ~shapes ~rename typ default in
          let default =
            if List.mem_assoc "smithy.api#clientOptional" traits' then
              [%expr Some [%e default]]
            else default
          in
          B.pexp_fun
            (Optional (field_name nm))
            (Some default)
            (B.ppat_var (Location.mknoloc (field_name nm)))
            expr
      | _ ->
          let optional = optional_member field in
          B.pexp_fun
            (if optional then Optional (field_name nm)
             else Labelled (field_name nm))
            None
            (B.ppat_var (Location.mknoloc (field_name nm)))
            expr)
    fields
    (if has_optionals || fields = [] then
       B.pexp_fun Nolabel None [%pat? ()] body
     else body)

let print_constructor ~shapes ~rename name fields =
  let body =
    B.pexp_constraint
      (B.pexp_record
         (List.map
            (fun (nm, _, _) ->
              let label = Location.mknoloc (Longident.Lident (field_name nm)) in
              (label, B.pexp_ident label))
            fields)
         None)
      (type_ident ~rename name)
  in
  let expr = constructor_parameters ~shapes ~rename ~fields ~body in
  [%stri
    let [%p B.ppat_var (Location.mknoloc (type_name ~rename name))] = [%e expr]]

let print_constructors ~shapes ~rename =
  IdMap.fold
    (fun name (sh, _) rem ->
      match sh with
      | Structure l when l <> [] ->
          print_constructor ~shapes ~rename name l :: rem
      | _ -> rem)
    shapes []

let type_constructor ~rename ~info ?arg_type nm =
  {
    (B.constructor_declaration
       ~args:
         (Pcstr_tuple
            (match arg_type with
            | None -> []
            | Some typ -> [ type_ident ~rename typ ]))
       ~name:(Location.mknoloc (constr_name nm))
       ~res:None)
    with
    pcd_attributes = info;
  }

let print_type ?heading ~inputs ~shapes ~rename (nm, (sh, traits)) =
  let text = match heading with None -> [] | Some h -> [ text_attr h ] in
  let docs =
    documentation ~shapes traits
    @
    match sh with
    | Structure l when l <> [] && IdSet.mem nm inputs ->
        [
          doc_attr
            ("See associated record builder function {!val:"
           ^ type_name ~rename nm ^ "}.");
        ]
    | _ -> []
  in
  let manifest_type manifest =
    B.type_declaration ~manifest:(Some manifest)
      ~name:(Location.mknoloc (type_name ~rename nm))
      ~params:[] ~cstrs:[] ~kind:Ptype_abstract ~private_:Public
  in
  {
    (match sh with
    | Blob -> manifest_type [%type: string]
    | Boolean -> manifest_type [%type: bool]
    | String -> manifest_type [%type: string]
    | Enum l ->
        let l =
          List.map
            (fun (nm, _, traits) ->
              type_constructor ~rename ~info:(documentation ~shapes traits) nm)
            l
        in
        B.type_declaration ~manifest:None ~kind:(Ptype_variant l)
          ~name:(Location.mknoloc (type_name ~rename nm))
          ~params:[] ~cstrs:[] ~private_:Public
    | Integer | IntEnum | Short -> manifest_type [%type: int]
    | Long -> manifest_type [%type: Int64.t]
    | Float | Double -> manifest_type [%type: float]
    | Byte -> manifest_type [%type: char]
    | Timestamp -> manifest_type [%type: CalendarLib.Calendar.t]
    | Document -> manifest_type [%type: Yojson.Safe.t]
    | List (id, _) ->
        let sparse = List.mem_assoc "smithy.api#sparse" traits in
        let id = type_ident ~rename id in
        manifest_type
          [%type: [%t if sparse then [%type: [%t id] option] else id] list]
    | Map ((key, _), (value, _)) ->
        let sparse = List.mem_assoc "smithy.api#sparse" traits in
        let id = type_ident ~rename value in
        manifest_type
          [%type:
            ( [%t type_ident ~rename key],
              [%t if sparse then [%type: [%t id] option] else id] )
            Converters.map]
    | Structure [] -> manifest_type [%type: unit]
    | Structure l ->
        let l =
          List.map
            (fun ((nm, typ, traits) as field) ->
              let optional = optional_member field in
              let id = type_ident ~rename typ in
              {
                (B.label_declaration ~mutable_:Immutable
                   ~name:(Location.mknoloc (field_name nm))
                   ~type_:(if optional then [%type: [%t id] option] else id))
                with
                pld_attributes = documentation ~shapes traits;
              })
            l
        in
        B.type_declaration ~manifest:None ~kind:(Ptype_record l)
          ~name:(Location.mknoloc (type_name ~rename nm))
          ~params:[] ~cstrs:[] ~private_:Public
    | Union l ->
        let l =
          List.map
            (fun (nm, typ, traits) ->
              let arg_type = if typ = unit_type then None else Some typ in
              type_constructor ~rename
                ~info:(documentation ~shapes traits)
                ?arg_type nm)
            l
        in
        B.type_declaration ~manifest:None ~kind:(Ptype_variant l)
          ~name:(Location.mknoloc (type_name ~rename nm))
          ~params:[] ~cstrs:[] ~private_:Public
    | Service _ | Resource _ | Operation _ -> assert false)
    with
    ptype_attributes = text @ docs;
  }

let print_types ~rename ~inputs ~outputs ~operations shapes =
  let types =
    IdMap.bindings
      (IdMap.filter
         (fun id (typ, _) ->
           match typ with
           | Service _ | Operation _ | Resource _ -> false
           | _ ->
               IdSet.mem id inputs || IdSet.mem id outputs
               || IdSet.mem id operations)
         shapes)
  in
  let operations, types =
    List.partition (fun (nm, _) -> IdSet.mem nm operations) types
  in
  let errors, types =
    List.partition
      (fun (_, (_, traits)) -> List.mem_assoc "smithy.api#error" traits)
      types
  in
  let print_types heading types =
    List.mapi
      (fun i err ->
        let heading = if i = 0 then Some heading else None in
        print_type ?heading ~inputs ~shapes ~rename err)
      types
  in
  B.pstr_type Recursive
    (print_types "{2 Operations}" operations
    @ print_types "{2 Additional types}" types
    @ print_types "{2 Errors}" errors)

let ident id = B.pexp_ident (Location.mknoloc (Longident.Lident id))

let converter ~rename ~path ~sparse id =
  let convert id =
    match id.namespace with
    | "smithy.api" ->
        B.pexp_ident
          (Location.mknoloc
             (Longident.Ldot
                ( path,
                  match id.identifier with
                  | "Boolean" | "PrimitiveBoolean" -> "boolean"
                  | "Blob" -> "blob"
                  | "String" -> "string"
                  | "Integer" -> "integer"
                  | "Long" | "PrimitiveLong" -> "long"
                  | "Float" | "Double" -> "float"
                  | "Timestamp" -> "timestamp"
                  | "Document" -> "document"
                  | "Short" -> "integer"
                  | "Byte" -> "byte"
                  | _ -> assert false )))
    | _ -> ident (type_name ~rename id)
  in
  if sparse then
    [%expr
      [%e B.pexp_ident (Location.mknoloc (Longident.Ldot (path, "option")))]
        [%e convert id]]
  else convert id

let pat_construct nm =
  B.ppat_construct (Location.mknoloc (Longident.Lident (constr_name nm)))

let structure_json_converter ~rename ~fixed_fields:fixed fields =
  let path = Longident.(Ldot (Lident "Converters", "To_JSON")) in
  [%expr
    Converters.To_JSON.structure
      [%e
        List.fold_right
          (fun ((nm, typ, _) as field) lst ->
            let optional = optional_member field in
            [%expr
              ( [%e B.pexp_constant (const_string (member_name ~fixed field))],
                [%e converter ~rename ~path ~sparse:optional typ]
                  [%e ident (field_name nm ^ "'")] )
              :: [%e lst]])
          fields [%expr []]]]

let to_json ~rename ~fixed_fields (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "To_JSON")) in
  let nm = type_name ~rename name ^ "'" in
  B.value_binding
    ~pat:(B.ppat_var (Location.mknoloc (type_name ~rename name)))
    ~expr:
      (B.pexp_fun Nolabel None
         [%pat?
           ([%p B.ppat_var (Location.mknoloc nm)] :
             [%t type_ident ~rename name])]
         (B.pexp_constraint
            (match sh with
            | Blob -> [%expr Converters.To_JSON.blob [%e ident nm]]
            | Boolean -> [%expr Converters.To_JSON.boolean [%e ident nm]]
            | String -> [%expr Converters.To_JSON.string [%e ident nm]]
            | Enum l ->
                [%expr
                  Converters.To_JSON.string
                    [%e
                      B.pexp_match (ident nm)
                        (List.map
                           (fun ((nm, _, _) as enum) ->
                             B.case ~lhs:(pat_construct nm None) ~guard:None
                               ~rhs:
                                 (B.pexp_constant
                                    (const_string
                                       (member_name ~fixed:false
                                          ~name:"smithy.api#enumValue" enum))))
                           l)]]
            | Integer | IntEnum | Short ->
                [%expr Converters.To_JSON.integer [%e ident nm]]
            | Long -> [%expr Converters.To_JSON.long [%e ident nm]]
            | Float | Double -> [%expr Converters.To_JSON.float [%e ident nm]]
            | Byte -> [%expr Converters.To_JSON.byte [%e ident nm]]
            | Timestamp -> [%expr Converters.To_JSON.timestamp [%e ident nm]]
            | Document -> [%expr Converters.To_JSON.document [%e ident nm]]
            | List (id, _) ->
                let sparse = List.mem_assoc "smithy.api#sparse" traits in
                [%expr
                  Converters.To_JSON.list
                    [%e converter ~rename ~path ~sparse id]
                    [%e ident nm]]
            | Map (_key, (value, _)) ->
                let sparse = List.mem_assoc "smithy.api#sparse" traits in
                [%expr
                  Converters.To_JSON.map
                    [%e converter ~rename ~path ~sparse value]
                    [%e ident nm]]
            | Structure [] -> [%expr Converters.To_JSON.structure []]
            | Structure l ->
                B.pexp_match (ident nm)
                  [
                    B.case
                      ~lhs:
                        (B.ppat_record
                           (List.map
                              (fun (nm, _, _) ->
                                ( Location.mknoloc
                                    (Longident.Lident (field_name nm)),
                                  B.ppat_var
                                    (Location.mknoloc (field_name nm ^ "'")) ))
                              l)
                           Closed)
                      ~guard:None
                      ~rhs:(structure_json_converter ~rename ~fixed_fields l);
                  ]
            | Union l ->
                [%expr
                  Converters.To_JSON.structure
                    [
                      [%e
                        B.pexp_match (ident nm)
                          (List.map
                             (fun ((nm, typ, _) as constr) ->
                               if typ = unit_type then
                                 B.case ~lhs:(pat_construct nm None) ~guard:None
                                   ~rhs:
                                     [%expr
                                       [%e
                                         B.pexp_constant
                                           (const_string
                                              (member_name ~fixed:fixed_fields
                                                 constr))],
                                         [%e
                                           structure_json_converter ~rename
                                             ~fixed_fields []]]
                               else
                                 B.case
                                   ~lhs:(pat_construct nm (Some [%pat? x]))
                                   ~guard:None
                                   ~rhs:
                                     [%expr
                                       [%e
                                         B.pexp_constant
                                           (const_string
                                              (member_name ~fixed:fixed_fields
                                                 constr))],
                                         [%e
                                           (converter ~rename ~path
                                              ~sparse:false)
                                             typ]
                                           x])
                             l)];
                    ]]
            | Service _ | Resource _ | Operation _ -> assert false)
            [%type: Yojson.Safe.t]))

let print_to_json ~rename ~fixed_fields shs =
  [%str
    module To_JSON = struct
      [%%i
      B.pstr_value Recursive
        (List.map (to_json ~fixed_fields ~rename) (IdMap.bindings shs))]
    end]

let field_xml_converter ~rename ?param ((nm, typ, _) as field) =
  let path = Longident.(Ldot (Lident "Converters", "To_XML")) in
  let optional = param = None && optional_member field in
  let name =
    B.pexp_constant
      (const_string (member_name ~fixed:false ~name:"smithy.api#xmlName" field))
  in
  let flat = flattened_member field in

  let conv = converter ~rename ~path ~sparse:false typ in
  let id = ident (Option.value ~default:(field_name nm ^ "'") param) in
  let field_builder =
    if flat then [%expr [%e conv] ~flat:[%e name]]
    else [%expr Converters.To_XML.field [%e name] [%e conv]]
  in
  if optional then [%expr Converters.To_XML.option [%e field_builder] [%e id]]
  else [%expr [%e field_builder] [%e id]]

let structure_xml_converter ~rename fields =
  [%expr
    Converters.To_XML.structure
      [%e
        List.fold_right
          (fun field lst ->
            [%expr [%e field_xml_converter ~rename field] :: [%e lst]])
          fields [%expr []]]]

let to_xml ~rename (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "To_XML")) in
  let nm = type_name ~rename name ^ "'" in
  let wrap e =
    match sh with List _ | Map _ -> [%expr fun ?flat -> [%e e]] | _ -> e
  in
  B.value_binding
    ~pat:(B.ppat_var (Location.mknoloc (type_name ~rename name)))
    ~expr:
      (wrap
         (B.pexp_fun Nolabel None
            [%pat?
              ([%p B.ppat_var (Location.mknoloc nm)] :
                [%t type_ident ~rename name])]
            (B.pexp_constraint
               (match sh with
               | Blob -> [%expr Converters.To_XML.blob [%e ident nm]]
               | Boolean -> [%expr Converters.To_XML.boolean [%e ident nm]]
               | String -> [%expr Converters.To_XML.string [%e ident nm]]
               | Enum l ->
                   [%expr
                     Converters.To_XML.string
                       [%e
                         B.pexp_match (ident nm)
                           (List.map
                              (fun ((nm, _, _) as enum) ->
                                B.case ~lhs:(pat_construct nm None) ~guard:None
                                  ~rhs:
                                    (B.pexp_constant
                                       (const_string
                                          (member_name ~fixed:false
                                             ~name:"smithy.api#enumValue" enum))))
                              l)]]
               | Integer | IntEnum | Short ->
                   [%expr Converters.To_XML.integer [%e ident nm]]
               | Long -> [%expr Converters.To_XML.long [%e ident nm]]
               | Float | Double -> [%expr Converters.To_XML.float [%e ident nm]]
               | Byte -> [%expr Converters.To_XML.byte [%e ident nm]]
               | Timestamp -> [%expr Converters.To_XML.timestamp [%e ident nm]]
               | Document -> assert false
               | List (id, traits') ->
                   let sparse = List.mem_assoc "smithy.api#sparse" traits in
                   assert (not sparse);
                   let name =
                     Option.map Yojson.Safe.Util.to_string
                       (List.assoc_opt "smithy.api#xmlName" traits')
                   in
                   ( [%expr Converters.To_XML.list] |> fun e ->
                     match name with
                     | None -> e
                     | Some name ->
                         [%expr
                           [%e e] ~name:[%e B.pexp_constant (const_string name)]]
                   )
                   |> fun e ->
                   [%expr
                     [%e e]
                       [%e converter ~rename ~path ~sparse id]
                       ?flat [%e ident nm]]
               | Map ((_key, key_traits), (value, value_traits)) ->
                   let sparse = List.mem_assoc "smithy.api#sparse" traits in
                   assert (not sparse);
                   let key_name =
                     Option.map Yojson.Safe.Util.to_string
                       (List.assoc_opt "smithy.api#xmlName" key_traits)
                   in
                   let value_name =
                     Option.map Yojson.Safe.Util.to_string
                       (List.assoc_opt "smithy.api#xmlName" value_traits)
                   in
                   ( ( [%expr Converters.To_XML.map] |> fun e ->
                       match key_name with
                       | None -> e
                       | Some nm ->
                           [%expr
                             [%e e]
                               ~key_name:[%e B.pexp_constant (const_string nm)]]
                     )
                   |> fun e ->
                     match value_name with
                     | None -> e
                     | Some nm ->
                         [%expr
                           [%e e]
                             ~value_name:[%e B.pexp_constant (const_string nm)]]
                   )
                   |> fun e ->
                   [%expr
                     [%e e]
                       [%e converter ~rename ~path ~sparse value]
                       ?flat [%e ident nm]]
               | Structure [] -> [%expr Converters.To_XML.structure []]
               | Structure l ->
                   B.pexp_match (ident nm)
                     [
                       B.case
                         ~lhs:
                           (B.ppat_record
                              (List.map
                                 (fun (nm, _, _) ->
                                   ( Location.mknoloc
                                       (Longident.Lident (field_name nm)),
                                     B.ppat_var
                                       (Location.mknoloc (field_name nm ^ "'"))
                                   ))
                                 l)
                              Closed)
                         ~guard:None
                         ~rhs:(structure_xml_converter ~rename l);
                     ]
               | Union l ->
                   [%expr
                     Converters.To_XML.structure
                       [
                         [%e
                           B.pexp_match (ident nm)
                             (List.map
                                (fun ((nm, _, _) as constr) ->
                                  B.case
                                    ~lhs:(pat_construct nm (Some [%pat? x]))
                                    ~guard:None
                                    ~rhs:
                                      (field_xml_converter ~rename ~param:"x"
                                         constr))
                                l)];
                       ]]
               | Service _ | Resource _ | Operation _ -> assert false)
               [%type: Converters.To_XML.t])))

let print_to_xml ~rename shs =
  [%str
    module To_XML = struct
      [%%i
      B.pstr_value Recursive (List.map (to_xml ~rename) (IdMap.bindings shs))]
    end]

let query_alt_name ~protocol ~traits =
  match
    if protocol <> `Ec2Query then None
    else List.assoc_opt "aws.protocols#ec2QueryName" traits
  with
  | Some name -> Some (Yojson.Safe.Util.to_string name)
  | None -> (
      match List.assoc_opt "smithy.api#xmlName" traits with
      | Some name -> Some (Yojson.Safe.Util.to_string name)
      | None -> None)

let field_graph_converter ~rename ~protocol ?param ((nm, typ, traits) as field)
    =
  let path = Longident.(Ldot (Lident "Converters", "To_Graph")) in
  let optional = param = None && optional_member field in
  let name =
    let name = Option.value ~default:nm (query_alt_name ~protocol ~traits) in
    let name =
      if protocol = `Ec2Query then String.capitalize_ascii name else name
    in
    B.pexp_constant (const_string name)
  in
  let flat = flattened_member field in

  let conv = converter ~rename ~path ~sparse:false typ in
  let id = ident (Option.value ~default:(field_name nm ^ "'") param) in
  let field_builder =
    [%expr
      Converters.To_Graph.field [%e name]
        [%e if flat then [%expr [%e conv] ~flat:true] else [%expr [%e conv]]]]
  in
  if optional then [%expr Converters.To_Graph.option [%e field_builder] [%e id]]
  else [%expr [%e field_builder] [%e id]]

let structure_graph_converter ~rename ~protocol fields =
  [%expr
    Converters.To_Graph.structure
      [%e
        List.fold_right
          (fun field lst ->
            [%expr
              [%e field_graph_converter ~rename ~protocol field] :: [%e lst]])
          fields [%expr []]]]

let to_graph ~rename ~protocol (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "To_Graph")) in
  let nm = type_name ~rename name ^ "'" in
  let wrap e =
    match sh with List _ | Map _ -> [%expr fun ?flat -> [%e e]] | _ -> e
  in
  B.value_binding
    ~pat:(B.ppat_var (Location.mknoloc (type_name ~rename name)))
    ~expr:
      (wrap
         (B.pexp_fun Nolabel None
            [%pat?
              ([%p B.ppat_var (Location.mknoloc nm)] :
                [%t type_ident ~rename name])]
            (B.pexp_constraint
               (match sh with
               | Blob -> [%expr Converters.To_Graph.blob [%e ident nm]]
               | Boolean -> [%expr Converters.To_Graph.boolean [%e ident nm]]
               | String -> [%expr Converters.To_Graph.string [%e ident nm]]
               | Enum l ->
                   [%expr
                     Converters.To_Graph.string
                       [%e
                         B.pexp_match (ident nm)
                           (List.map
                              (fun ((nm, _, _) as enum) ->
                                B.case ~lhs:(pat_construct nm None) ~guard:None
                                  ~rhs:
                                    (B.pexp_constant
                                       (const_string
                                          (member_name ~fixed:false
                                             ~name:"smithy.api#enumValue" enum))))
                              l)]]
               | Integer | IntEnum | Short ->
                   [%expr Converters.To_Graph.integer [%e ident nm]]
               | Long -> [%expr Converters.To_Graph.long [%e ident nm]]
               | Float | Double ->
                   [%expr Converters.To_Graph.float [%e ident nm]]
               | Byte -> [%expr Converters.To_Graph.byte [%e ident nm]]
               | Timestamp ->
                   [%expr Converters.To_Graph.timestamp [%e ident nm]]
               | Document -> [%expr Converters.To_Graph.document [%e ident nm]]
               | List (id, traits') ->
                   let sparse = List.mem_assoc "smithy.api#sparse" traits in
                   assert (not sparse);
                   let name = query_alt_name ~protocol ~traits:traits' in
                   ( [%expr Converters.To_Graph.list] |> fun e ->
                     match name with
                     | None -> e
                     | Some name ->
                         [%expr
                           [%e e] ~name:[%e B.pexp_constant (const_string name)]]
                   )
                   |> fun e ->
                   [%expr
                     [%e e]
                       [%e converter ~rename ~path ~sparse id]
                       ?flat [%e ident nm]]
               | Map ((_key, key_traits), (value, value_traits)) ->
                   let sparse = List.mem_assoc "smithy.api#sparse" traits in
                   assert (not sparse);
                   let name = query_alt_name ~protocol ~traits in
                   let key_name = query_alt_name ~protocol ~traits:key_traits in
                   let value_name =
                     query_alt_name ~protocol ~traits:value_traits
                   in
                   ( ( ( [%expr Converters.To_Graph.map] |> fun e ->
                         match name with
                         | None -> e
                         | Some nm ->
                             [%expr
                               [%e e]
                                 ~name:[%e B.pexp_constant (const_string nm)]]
                       )
                     |> fun e ->
                       match key_name with
                       | None -> e
                       | Some nm ->
                           [%expr
                             [%e e]
                               ~key_name:[%e B.pexp_constant (const_string nm)]]
                     )
                   |> fun e ->
                     match value_name with
                     | None -> e
                     | Some nm ->
                         [%expr
                           [%e e]
                             ~value_name:[%e B.pexp_constant (const_string nm)]]
                   )
                   |> fun e ->
                   [%expr
                     [%e e]
                       [%e converter ~rename ~path ~sparse value]
                       ?flat [%e ident nm]]
               | Structure [] -> [%expr Converters.To_Graph.structure []]
               | Structure l ->
                   B.pexp_match (ident nm)
                     [
                       B.case
                         ~lhs:
                           (B.ppat_record
                              (List.map
                                 (fun (nm, _, _) ->
                                   ( Location.mknoloc
                                       (Longident.Lident (field_name nm)),
                                     B.ppat_var
                                       (Location.mknoloc (field_name nm ^ "'"))
                                   ))
                                 l)
                              Closed)
                         ~guard:None
                         ~rhs:(structure_graph_converter ~rename ~protocol l);
                     ]
               | Union l ->
                   [%expr
                     Converters.To_Graph.structure
                       [
                         [%e
                           B.pexp_match (ident nm)
                             (List.map
                                (fun ((nm, _, _) as constr) ->
                                  B.case
                                    ~lhs:(pat_construct nm (Some [%pat? x]))
                                    ~guard:None
                                    ~rhs:
                                      (field_graph_converter ~rename ~protocol
                                         ~param:"x" constr))
                                l)];
                       ]]
               | Service _ | Resource _ | Operation _ -> assert false)
               [%type: Converters.To_Graph.t])))

let print_to_graph ~rename ~protocol shs =
  [%str
    module To_Graph = struct
      [%%i
      B.pstr_value Recursive
        (List.map (to_graph ~rename ~protocol) (IdMap.bindings shs))]
    end]

let from_json ~rename ~fixed_fields (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "From_JSON")) in
  let nm = type_name ~rename name ^ "'" in
  B.value_binding
    ~pat:(B.ppat_var (Location.mknoloc (type_name ~rename name)))
    ~expr:
      (B.pexp_fun Nolabel None
         [%pat? ([%p B.ppat_var (Location.mknoloc nm)] : Yojson.Safe.t)]
         (B.pexp_constraint
            (match sh with
            | Blob -> [%expr Converters.From_JSON.blob [%e ident nm]]
            | Boolean -> [%expr Converters.From_JSON.boolean [%e ident nm]]
            | String -> [%expr Converters.From_JSON.string [%e ident nm]]
            | Enum l ->
                B.pexp_match
                  [%expr Converters.From_JSON.string [%e ident nm]]
                  (List.map
                     (fun ((nm, _, _) as enum) ->
                       B.case
                         ~lhs:
                           (B.ppat_constant
                              (const_string
                                 (member_name ~fixed:false
                                    ~name:"smithy.api#enumValue" enum)))
                         ~guard:None
                         ~rhs:
                           (B.pexp_construct
                              (Location.mknoloc
                                 (Longident.Lident (constr_name nm)))
                              None))
                     l
                  @ [
                      B.case
                        ~lhs:[%pat? _]
                        ~guard:None
                        ~rhs:[%expr assert false];
                    ])
            | Integer | IntEnum | Short ->
                [%expr Converters.From_JSON.integer [%e ident nm]]
            | Long -> [%expr Converters.From_JSON.long [%e ident nm]]
            | Float | Double -> [%expr Converters.From_JSON.float [%e ident nm]]
            | Byte -> [%expr Converters.From_JSON.byte [%e ident nm]]
            | Timestamp -> [%expr Converters.From_JSON.timestamp [%e ident nm]]
            | Document -> [%expr Converters.From_JSON.document [%e ident nm]]
            | List (id, _) ->
                let sparse = List.mem_assoc "smithy.api#sparse" traits in
                [%expr
                  Converters.From_JSON.list
                    [%e converter ~rename ~path ~sparse id]
                    [%e ident nm]]
            | Map (_key, (value, _)) ->
                let sparse = List.mem_assoc "smithy.api#sparse" traits in
                [%expr
                  Converters.From_JSON.map
                    [%e converter ~rename ~path ~sparse value]
                    [%e ident nm]]
            | Structure [] -> [%expr ()]
            | Structure l ->
                [%expr
                  let x = Converters.From_JSON.structure [%e ident nm] in
                  [%e
                    B.pexp_record
                      (List.map
                         (fun ((nm, typ, _) as field) ->
                           let optional = optional_member field in
                           let label =
                             Location.mknoloc (Longident.Lident (field_name nm))
                           in
                           ( label,
                             [%expr
                               [%e converter ~rename ~path ~sparse:optional typ]
                                 (Converters.From_JSON.field
                                    [%e
                                      B.pexp_constant
                                        (const_string
                                           (member_name ~fixed:fixed_fields
                                              field))]
                                    x)] ))
                         l)
                      None]]
            | Union l ->
                B.pexp_match
                  [%expr Converters.From_JSON.structure [%e ident nm]]
                  (List.map
                     (fun ((nm, typ, _) as constr) ->
                       if typ = unit_type then
                         B.case
                           ~lhs:
                             [%pat?
                               ( [%p
                                   B.ppat_constant
                                     (const_string
                                        (member_name ~fixed:fixed_fields constr))],
                                 _ )
                               :: _]
                           ~guard:None
                           ~rhs:
                             (B.pexp_construct
                                (Location.mknoloc
                                   (Longident.Lident (constr_name nm)))
                                None)
                       else
                         B.case
                           ~lhs:
                             [%pat?
                               ( [%p
                                   B.ppat_constant
                                     (const_string
                                        (member_name ~fixed:fixed_fields constr))],
                                 x )
                               :: _]
                           ~guard:None
                           ~rhs:
                             (B.pexp_construct
                                (Location.mknoloc
                                   (Longident.Lident (constr_name nm)))
                                (Some
                                   [%expr
                                     [%e
                                       (converter ~rename ~path ~sparse:false)
                                         typ]
                                       x])))
                     l
                  @ [
                      (* ignore unknown properties *)
                      B.case
                        ~lhs:[%pat? _ :: r]
                        ~guard:None
                        ~rhs:
                          [%expr [%e ident (type_name ~rename name)] (`Assoc r)];
                      B.case
                        ~lhs:[%pat? []]
                        ~guard:None
                        ~rhs:[%expr assert false];
                    ])
            | Service _ | Resource _ | Operation _ -> assert false)
            (type_ident ~rename name)))

let print_from_json ~rename ~fixed_fields shs =
  [%str
    module From_JSON = struct
      [%%i
      B.pstr_value Recursive
        (List.map (from_json ~rename ~fixed_fields) (IdMap.bindings shs))]
    end]

let from_xml ~rename (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "From_XML")) in
  let nm = type_name ~rename name ^ "'" in
  let wrap e =
    match sh with List _ | Map _ -> [%expr fun ?flat -> [%e e]] | _ -> e
  in
  B.value_binding
    ~pat:(B.ppat_var (Location.mknoloc (type_name ~rename name)))
    ~expr:
      (wrap
         (B.pexp_fun Nolabel None
            [%pat?
              ([%p B.ppat_var (Location.mknoloc nm)] : Converters.From_XML.t)]
            (B.pexp_constraint
               (match sh with
               | Blob -> [%expr Converters.From_XML.blob [%e ident nm]]
               | Boolean -> [%expr Converters.From_XML.boolean [%e ident nm]]
               | String -> [%expr Converters.From_XML.string [%e ident nm]]
               | Enum l ->
                   B.pexp_match
                     [%expr Converters.From_XML.string [%e ident nm]]
                     (List.map
                        (fun ((nm, _, _) as enum) ->
                          B.case
                            ~lhs:
                              (B.ppat_constant
                                 (const_string
                                    (member_name ~fixed:false
                                       ~name:"smithy.api#enumValue" enum)))
                            ~guard:None
                            ~rhs:
                              (B.pexp_construct
                                 (Location.mknoloc
                                    (Longident.Lident (constr_name nm)))
                                 None))
                        l
                     @ [
                         B.case
                           ~lhs:[%pat? _]
                           ~guard:None
                           ~rhs:[%expr assert false];
                       ])
               | Integer | IntEnum | Short ->
                   [%expr Converters.From_XML.integer [%e ident nm]]
               | Long -> [%expr Converters.From_XML.long [%e ident nm]]
               | Float | Double ->
                   [%expr Converters.From_XML.float [%e ident nm]]
               | Byte -> [%expr Converters.From_XML.byte [%e ident nm]]
               | Timestamp ->
                   [%expr Converters.From_XML.timestamp [%e ident nm]]
               | Document -> assert false
               | List (id, traits') ->
                   let sparse = List.mem_assoc "smithy.api#sparse" traits in
                   assert (not sparse);
                   let name =
                     Option.map Yojson.Safe.Util.to_string
                       (List.assoc_opt "smithy.api#xmlName" traits')
                   in
                   ( [%expr Converters.From_XML.list] |> fun e ->
                     match name with
                     | None -> e
                     | Some name ->
                         [%expr
                           [%e e] ~name:[%e B.pexp_constant (const_string name)]]
                   )
                   |> fun e ->
                   [%expr
                     [%e e]
                       [%e converter ~rename ~path ~sparse id]
                       ?flat [%e ident nm]]
               | Map ((_key, key_traits), (value, value_traits)) ->
                   let sparse = List.mem_assoc "smithy.api#sparse" traits in
                   assert (not sparse);
                   let key_name =
                     Option.map Yojson.Safe.Util.to_string
                       (List.assoc_opt "smithy.api#xmlName" key_traits)
                   in
                   let value_name =
                     Option.map Yojson.Safe.Util.to_string
                       (List.assoc_opt "smithy.api#xmlName" value_traits)
                   in
                   ( ( [%expr Converters.From_XML.map] |> fun e ->
                       match key_name with
                       | None -> e
                       | Some nm ->
                           [%expr
                             [%e e]
                               ~key_name:[%e B.pexp_constant (const_string nm)]]
                     )
                   |> fun e ->
                     match value_name with
                     | None -> e
                     | Some nm ->
                         [%expr
                           [%e e]
                             ~value_name:[%e B.pexp_constant (const_string nm)]]
                   )
                   |> fun e ->
                   [%expr
                     [%e e]
                       [%e converter ~rename ~path ~sparse value]
                       [%e ident nm]]
               | Structure [] -> [%expr ()]
               | Structure l ->
                   let x = ident nm in
                   [%expr
                     [%e
                       B.pexp_record
                         (List.map
                            (fun ((nm, typ, _) as field) ->
                              ( Location.mknoloc
                                  (Longident.Lident (field_name nm)),
                                let optional = optional_member field in
                                let name =
                                  B.pexp_constant
                                    (const_string
                                       (member_name ~fixed:false
                                          ~name:"smithy.api#xmlName" field))
                                in
                                let flat = flattened_member field in
                                let conv =
                                  converter ~rename ~path ~sparse:false typ
                                in
                                if flat then
                                  let e =
                                    [%expr
                                      Converters.From_XML.flattened_field
                                        [%e name] [%e conv] [%e x]]
                                  in
                                  if optional then [%expr Some [%e e]] else e
                                else
                                  [%expr
                                    [%e
                                      if optional then
                                        [%expr Converters.From_XML.opt_field]
                                      else [%expr Converters.From_XML.field]]
                                      [%e name] [%e conv] [%e x]] ))
                            l)
                         None]]
               | Union l ->
                   B.pexp_match
                     [%expr Converters.From_XML.union [%e ident nm]]
                     (List.map
                        (fun ((nm, typ, _) as constr) ->
                          B.case
                            ~lhs:
                              [%pat?
                                ( [%p
                                    B.ppat_constant
                                      (const_string
                                         (member_name ~fixed:false
                                            ~name:"smithy.api#xmlName" constr))],
                                  x )]
                            ~guard:None
                            ~rhs:
                              (B.pexp_construct
                                 (Location.mknoloc
                                    (Longident.Lident (constr_name nm)))
                                 (Some
                                    [%expr
                                      [%e
                                        (converter ~rename ~path ~sparse:false)
                                          typ]
                                        x])))
                        l
                     @ [
                         B.case
                           ~lhs:[%pat? _]
                           ~guard:None
                           ~rhs:[%expr assert false];
                       ])
               | Service _ | Resource _ | Operation _ -> assert false)
               (type_ident ~rename name))))

let print_from_xml ~rename shs =
  [%str
    module From_XML = struct
      [%%i
      B.pstr_value Recursive (List.map (from_xml ~rename) (IdMap.bindings shs))]
    end]

let compute_inputs_outputs shapes (id, _) =
  let rec traverse set ~direct id =
    if IdSet.mem id set then set
    else
      let set = if direct then set else IdSet.add id set in
      match IdMap.find_opt id shapes with
      | None -> set
      | Some (typ, _) -> (
          match typ with
          | Blob | Boolean | String | Enum _ | Integer | IntEnum | Long | Float
          | Double | Short | Byte | Timestamp | Document ->
              set
          | Service _ | Resource _ | Operation _ -> assert false
          | List (id, _) -> traverse set ~direct:false id
          | Map ((id, _), (id', _)) ->
              traverse (traverse set ~direct:false id) ~direct:false id'
          | Structure l | Union l ->
              List.fold_left
                (fun set (_, id, _) -> traverse set ~direct:false id)
                set l)
  in
  let traverse_list set lst =
    List.fold_left (fun set x -> traverse set ~direct:false x) set lst
  in
  let rec traverse_resource sets id =
    let ty, _ = IdMap.find id shapes in
    match ty with
    | Service { operations; resources; errors; _ } ->
        let inputs, outputs, operations =
          traverse_resources (traverse_resources sets operations) resources
        in
        (inputs, traverse_list outputs errors, operations)
    | Operation { input; output; errors; _ } ->
        let inputs, outputs, operations = sets in
        ( traverse inputs ~direct:true input,
          traverse_list (traverse outputs ~direct:false output) errors,
          IdSet.add input (IdSet.add output operations) )
    | Resource r ->
        let opt x = match x with None -> [] | Some x -> [ x ] in
        traverse_resources sets
          (opt r.create @ opt r.put @ opt r.read @ opt r.update @ opt r.delete
         @ opt r.list @ r.operations @ r.collection_operations @ r.resources)
    | _ -> assert false
  and traverse_resources sets lst =
    List.fold_left (fun sets x -> traverse_resource sets x) sets lst
  in
  traverse_resource (IdSet.empty, IdSet.empty, IdSet.empty) id

(*
let compile_rest_operation ~shapes nm input output errors traits =
  prerr_endline nm.identifier;
  if List.mem_assoc "smithy.api#http" traits then
    Format.eprintf "OOO %s@." (to_string (List.assoc "smithy.api#http" traits));
  let http = List.assoc "smithy.api#http" traits in
  let meth = Util.(http |> member "method" |> to_string) in
  prerr_endline meth;
  (*
    785 DELETE
   1894 GET
      1 HEAD
    188 PATCH
   2213 POST
    597 PUT
*)
  let uri = Util.(http |> member "uri" |> to_string) in
  prerr_endline uri;
  let code = Util.(http |> member "code" |> to_option to_int) in
  ignore (meth, uri, code);
  ignore (shapes, nm, input, output, errors, traits)
*)

(*
~builder:(fun k ~a ?(b =...) ~c -> k (To_Json.structure ...))
~errors:[("name", (retryable, fun x -> `Foo (From_Json.foo) x))); ...]
*)

let to_caml_list l =
  List.fold_right (fun e rem -> [%expr [%e e] :: [%e rem]]) l [%expr []]

let tag_re =
  Re.(
    compile
      (seq
         [
           char '{';
           group (rep1 (diff any (set "+#}")));
           opt (seq [ char '#'; group (rep1 (diff any (set "}"))) ]);
           opt (group (char '+'));
           char '}';
         ]))

let compile_string s f =
  match Re.split_full tag_re s with
  | [ `Text s ] -> B.pexp_constant (const_string s)
  | l ->
      let l =
        List.map
          (fun x ->
            match x with
            | `Text s -> B.pexp_constant (const_string s)
            | `Delim g ->
                f (Re.Group.get g 1) (Re.Group.get_opt g 2) (Re.Group.test g 3))
          l
      in
      [%expr String.concat "" [%e to_caml_list l]]

let rec compile_value ~shapes ~rename typ v =
  match type_of_shape shapes typ with
  | String | Blob -> B.pexp_constant (const_string (Util.to_string v))
  | Integer | IntEnum | Short ->
      B.pexp_constant (Pconst_integer (Int.to_string (Util.to_int v), None))
  | Long ->
      B.pexp_constant (Pconst_integer (Int.to_string (Util.to_int v), Some 'L'))
  | Boolean -> if Util.to_bool v then [%expr true] else [%expr false]
  | Byte -> B.pexp_constant (Pconst_char (Char.chr (Util.to_int v land 255)))
  | Float | Double -> (
      match v with
      | `String "NaN" -> [%expr Float.nan]
      | `String "Infinity" -> [%expr Float.infinity]
      | `String "-Infinity" -> [%expr Float.neg_infinity]
      | `Float f -> B.pexp_constant (Pconst_float (Printf.sprintf "%h" f, None))
      | _ -> assert false)
  | Document ->
      [%expr
        Yojson.Safe.from_string
          [%e B.pexp_constant (const_string (Yojson.Safe.to_string v))]]
  | Timestamp ->
      [%expr
        CalendarLib.Calendar.from_unixfloat
          [%e
            B.pexp_constant
              (Pconst_float (Printf.sprintf "%h" (Util.to_number v), None))]]
  | Structure [] -> [%expr ()]
  | Structure l ->
      B.pexp_record
        (List.map
           (fun ((nm, typ', traits') as field) ->
             let optional = optional_member field in
             let label = Location.mknoloc (Longident.Lident (field_name nm)) in
             let v' = Util.(v |> member nm) in
             ( label,
               if optional then
                 if v' = `Null then [%expr None]
                 else [%expr Some [%e compile_value ~shapes ~rename typ' v']]
               else
                 let v' =
                   if v' = `Null then List.assoc "smithy.api#default" traits'
                   else v'
                 in
                 compile_value ~shapes ~rename typ' v' ))
           l)
        None
  | Union l -> (
      match Util.to_assoc v with
      | [ (nm, v') ] ->
          let nm, typ', _ = List.find (fun (nm', _, _) -> nm = nm') l in
          B.pexp_construct
            (Location.mknoloc (Longident.Lident (constr_name nm)))
            (if typ' = unit_type then None
             else Some (compile_value ~shapes ~rename typ' v'))
      | _ -> assert false)
  | List (typ', _) ->
      let sparse =
        List.mem_assoc "smithy.api#sparse" (snd (IdMap.find typ shapes))
      in
      let l = Util.(v |> to_list) in
      let l =
        List.map
          (fun v ->
            if sparse then
              if v = `Null then [%expr None]
              else [%expr Some [%e compile_value ~shapes ~rename typ' v]]
            else compile_value ~shapes ~rename typ' v)
          l
      in
      List.fold_right (fun v rem -> [%expr [%e v] :: [%e rem]]) l [%expr []]
  | Map (_, (typ', _)) ->
      let sparse =
        List.mem_assoc "smithy.api#sparse" (snd (IdMap.find typ shapes))
      in
      let l = Util.(v |> to_assoc) in
      let l =
        List.map
          (fun (k, v) ->
            ( B.pexp_constant (const_string k),
              if sparse then
                if v = `Null then [%expr None]
                else
                  [%expr
                    Some [%e compile_value_with_type ~shapes ~rename typ' v]]
              else compile_value_with_type ~shapes ~rename typ' v ))
          l
      in
      List.fold_right
        (fun (k, v) rem ->
          [%expr Converters.StringMap.add [%e k] [%e v] [%e rem]])
        l [%expr Converters.StringMap.empty]
  | Enum l ->
      let nm = Util.to_string v in
      let nm', _, _ =
        List.find
          (fun enum ->
            member_name ~fixed:false ~name:"smithy.api#enumValue" enum = nm)
          l
      in
      B.pexp_construct
        (Location.mknoloc (Longident.Lident (constr_name nm')))
        None
  | _ -> assert false

and compile_value_with_type ~shapes ~rename typ v =
  B.pexp_constraint
    (compile_value ~shapes ~rename typ v)
    (type_ident ~rename typ)

let compile_parameters ~shapes ~rename typ v =
  if typ = unit_type || v = `Null then []
  else
    match type_of_shape shapes typ with
    | Structure l ->
        let pl = Util.to_assoc v in
        let pl = List.filter (fun (_, v') -> v' <> `Null) pl in
        let pl =
          List.map
            (fun (nm, v') ->
              let nm, typ', _ = List.find (fun (nm', _, _) -> nm = nm') l in
              (nm, compile_value_with_type ~shapes ~rename typ' v'))
            pl
        in
        List.map
          (fun (k, v) -> (Ppxlib.Ast.Labelled (uncapitalized_identifier k), v))
          pl
    | _ -> assert false

let compile_http_request_tests ~shapes ~rename ~input has_optionals op_name
    (test : http_request_test) =
  prerr_endline test.id;
  let add_test t f rem =
    match t with Some t -> [%expr [%e rem] && [%e f t]] | None -> rem
  in
  let t =
    [%expr
      let request =
        [%e
          B.pexp_apply
            [%expr ([%e ident (type_name ~rename op_name)] ()).builder]
            ((Ppxlib.Ast.Labelled "test", [%expr true])
             :: ( Nolabel,
                  [%expr
                    Uri.of_string
                      [%e
                        B.pexp_constant
                          (const_string
                             (match test.host with
                             | Some host ->
                                 (* Trailing slashes are optional, and the
                                    tests expect them. *)
                                 "https://"
                                 ^
                                 if String.ends_with host ~suffix:"/" then host
                                 else host ^ "/"
                             | None -> "https://unknownhost.com/"))]] )
             :: (Nolabel, [%expr fun x -> x])
             :: compile_parameters ~shapes ~rename input test.params
            @ if has_optionals then [ (Nolabel, [%expr ()]) ] else [])]
      in
      let request_headers =
        match request.body with
        | Some b ->
            ("Content-Length", string_of_int (String.length b))
            :: request.headers
        | None -> request.headers
      in
      ignore request_headers;
      [%e
        [%expr
          let path = [%e B.pexp_constant (const_string test.uri)] in
          let success = Uri.path request.uri = path in
          if not success then
            Format.eprintf "path: <%s> <%s>@." (Uri.path request.uri) path;
          success]
        |> add_test test.resolved_host (fun host ->
               [%expr
                 Uri.host request.uri
                 = Some [%e B.pexp_constant (const_string host)]])
        |> add_test
             (if test.forbid_query_params = [] then None
              else Some test.forbid_query_params)
             (fun params ->
               let params =
                 List.fold_right
                   (fun h rem ->
                     [%expr [%e B.pexp_constant (const_string h)] :: [%e rem]])
                   params [%expr []]
               in
               [%expr
                 let params = [%e params] in
                 let fail =
                   List.exists
                     (fun (h, _) -> List.mem h params)
                     (Uri.query request.uri)
                 in
                 if fail then (
                   List.iter
                     (fun (k, l) ->
                       List.iter (fun v -> Format.eprintf "%s:%s@." k v) l)
                     (Uri.query request.uri);
                   Format.eprintf "@.");
                 not fail])
        |> add_test
             (if test.require_query_params = [] then None
              else Some test.require_query_params)
             (fun params ->
               let params =
                 List.fold_right
                   (fun h rem ->
                     [%expr [%e B.pexp_constant (const_string h)] :: [%e rem]])
                   params [%expr []]
               in
               [%expr
                 let params = [%e params] in
                 let fail =
                   List.exists
                     (fun h -> not (List.mem_assoc h (Uri.query request.uri)))
                     headers
                 in
                 if fail then (
                   List.iter
                     (fun (k, l) ->
                       List.iter (fun v -> Format.eprintf "%s:%s@." k v) l)
                     (Uri.query request.uri);
                   Format.eprintf "@.");
                 not fail])
        |> add_test test.query_params (fun params ->
               let params =
                 List.fold_right
                   (fun v rem ->
                     [%expr [%e B.pexp_constant (const_string v)] :: [%e rem]])
                   params [%expr []]
               in
               [%expr
                 let params = [%e params] in
                 let fail =
                   List.exists
                     (fun p ->
                       match String.index p '=' with
                       | i ->
                           let k = String.sub p 0 i in
                           let v =
                             String.sub p (i + 1) (String.length p - i - 1)
                           in
                           List.assoc_opt k (Uri.query request.uri)
                           <> Some [ Uri.pct_decode v ]
                       | exception Not_found ->
                           List.assoc_opt p (Uri.query request.uri) <> Some [])
                     params
                 in
                 if fail then (
                   List.iter
                     (fun (k, l) ->
                       List.iter (fun v -> Format.eprintf "%s:%s@." k v) l)
                     (Uri.query request.uri);
                   Format.eprintf "@.");
                 not fail])
        |> add_test
             (if test.forbid_headers = [] then None
              else Some test.forbid_headers)
             (fun headers ->
               let headers =
                 List.fold_right
                   (fun h rem ->
                     [%expr [%e B.pexp_constant (const_string h)] :: [%e rem]])
                   headers [%expr []]
               in
               [%expr
                 let headers = [%e headers] in
                 let fail =
                   List.exists
                     (fun (h, _) -> List.mem h headers)
                     request_headers
                 in
                 if fail then (
                   Format.eprintf "Forbidden header@.";
                   List.iter
                     (fun (k, v) -> Format.eprintf "%s:%s@." k v)
                     request_headers;
                   Format.eprintf "@.");
                 not fail])
        |> add_test
             (if test.require_headers = [] then None
              else Some test.require_headers)
             (fun headers ->
               let headers =
                 List.fold_right
                   (fun h rem ->
                     [%expr [%e B.pexp_constant (const_string h)] :: [%e rem]])
                   headers [%expr []]
               in
               [%expr
                 let headers = [%e headers] in
                 let fail =
                   List.exists
                     (fun h -> not (List.mem_assoc h request_headers))
                     headers
                 in
                 if fail then (
                   Format.eprintf "Missing required header@.";
                   List.iter
                     (fun (k, v) -> Format.eprintf "%s:%s@." k v)
                     request_headers;
                   Format.eprintf "@.");
                 not fail])
        |> add_test test.method_ (fun meth ->
               [%expr
                 let success =
                   request.meth
                   = [%e
                       B.pexp_construct
                         (Location.mknoloc
                            (Longident.Lident (constr_name meth)))
                         None]
                 in
                 if not success then Format.eprintf "Bad method@.";
                 success])
        |> add_test test.headers (fun headers ->
               let headers =
                 List.fold_right
                   (fun (k, v) rem ->
                     [%expr
                       ( [%e B.pexp_constant (const_string k)],
                         [%e B.pexp_constant (const_string v)] )
                       :: [%e rem]])
                   headers [%expr []]
               in
               [%expr
                 let headers = [%e headers] in
                 let fail =
                   List.exists
                     (fun (k, v) -> List.assoc_opt k request_headers <> Some v)
                     headers
                 in
                 if fail then (
                   Format.eprintf "Header mismatch@.";
                   List.iter
                     (fun (k, v) -> Format.eprintf "%s:%s@." k v)
                     request_headers;
                   Format.eprintf "@.");
                 not fail])
        |> add_test test.body (fun body ->
               match test.body_media_type with
               | Some "application/json" ->
                   let body =
                     B.pexp_constant
                       (const_string
                          (Yojson.Safe.to_string (Yojson.Safe.from_string body)))
                   in
                   [%expr
                     let body = [%e body] in
                     let req_body = Option.value ~default:"" request.body in
                     if req_body <> body then
                       Format.eprintf "<%s> <%s>@." req_body body;
                     req_body = body]
               | None | Some ("application/octet-stream" | "image/jpg") ->
                   let body = B.pexp_constant (const_string body) in
                   [%expr
                     let body = [%e body] in
                     let req_body = Option.value ~default:"" request.body in
                     if req_body <> body then
                       Format.eprintf "<%s> <%s>@." req_body body;
                     req_body = body]
               | Some typ ->
                   prerr_endline typ;
                   assert false)]]
  in
  let t =
    match test.documentation with
    | None -> t
    | Some doc -> { t with pexp_attributes = [ text_attr doc ] }
  in
  [%stri let%test [%p B.ppat_constant (const_string test.id)] = [%e t]]

let compile_http_response_tests ~shapes ~rename ~output op_name
    (test : http_response_test) =
  prerr_endline test.id;
  (*
  let add_test t f rem =
    match t with Some t -> [%expr [%e rem] && [%e f t]] | None -> rem
  in
  *)
  let t =
    [%expr
      let response =
        ([%e ident (type_name ~rename op_name)] ()).parser
          {
            code =
              [%e
                B.pexp_constant (Pconst_integer (Int.to_string test.code, None))];
            body =
              [%e
                B.pexp_constant
                  (const_string (Option.value ~default:"" test.body))];
            headers =
              [%e
                List.fold_right
                  (fun (k, v) rem ->
                    [%expr
                      ( [%e B.pexp_constant (const_string k)],
                        [%e B.pexp_constant (const_string v)] )
                      :: [%e rem]])
                  (Option.value ~default:[] test.headers)
                  [%expr []]];
          }
      in
      match response with
      | Ok response ->
          let expected =
            [%e
              if output = unit_type then [%expr ()]
              else
                compile_value_with_type ~shapes ~rename output
                  (if test.params = `Null then `Assoc [] else test.params)]
          in
          (* We don't generate serializers for outputs
             ignore
               (let open! To_JSON in
               [%e ident (uncapitalized_identifier output.identifier)] expected);
          *)
          compare response expected = 0
      | _ ->
          prerr_endline "ERROR";
          false]
  in
  let t =
    match test.documentation with
    | None -> t
    | Some doc -> { t with pexp_attributes = [ text_attr doc ] }
  in
  [%stri let%test [%p B.ppat_constant (const_string test.id)] = [%e t]]

let convert_to_string ~shapes ~rename ~fields nm expr =
  let _, typ, _ = List.find (fun (nm', _, _) -> nm' = nm) fields in
  match type_of_shape shapes typ with
  | String -> expr
  | Boolean -> [%expr Converters.To_String.boolean [%e expr]]
  | Integer | Short -> [%expr Converters.To_String.integer [%e expr]]
  | Long -> [%expr Converters.To_String.long [%e expr]]
  | Byte -> [%expr Converters.To_String.byte [%e expr]]
  | Float | Double -> [%expr Converters.To_String.float [%e expr]]
  | Timestamp -> [%expr Converters.To_String.timestamp [%e expr]]
  | Enum _ ->
      [%expr
        Yojson.Safe.Util.to_string
          (To_JSON.([%e ident (type_name ~rename typ)]) [%e expr])]
  | _ ->
      Format.eprintf "ZZZ %s@." typ.identifier;
      [%expr
        ignore [%e expr];
        ""]

let compile_pattern ~shapes ~rename ~fields s =
  compile_string s @@ fun label property greedy ->
  assert (property = None);
  let s =
    convert_to_string ~shapes ~rename ~fields label
      (ident (field_name label ^ "'"))
  in
  if greedy then
    [%expr
      String.concat "/"
        (List.map Uri.pct_encode (String.split_on_char '/' [%e s]))]
  else [%expr Uri.pct_encode [%e s]]

let compile_rest_json_operation ~service_info ~shapes ~rename nm
    { input; output; errors; _ } traits =
  prerr_endline nm.identifier;
  let http = List.assoc "smithy.api#http" traits in
  let meth = Util.(http |> member "method" |> to_string) in
  let path = Util.(http |> member "uri" |> to_string) in
  let code = Util.(http |> member "code" |> to_option to_int) in
  (*ZZZ  assert (code = None || code = Some 200);*)
  ignore code;
  (*
  (match code with
  | Some 200 | None -> ()
  | Some code -> Format.eprintf "CODE %d@." code);
*)
  let field_refs name m =
    if name.namespace = "smithy.api" then m
    else
      match IdMap.find name shapes with
      | Structure l, _ ->
          List.fold_left
            (fun m (nm, _, _) -> StringMap.add nm name.identifier m)
            m l
      | _ -> assert false
  in
  let docs =
    (if input.namespace = "smithy.api" then []
     else
       [
         doc_attr
           ("See type {!type:" ^ type_name ~rename input
          ^ "} for a description of the parameters");
       ])
    @ documentation ~shapes
        ~field_refs:(StringMap.empty |> field_refs output |> field_refs input)
        traits
  in
  (* Method: POST, uri: / *)
  let host_prefix =
    Option.map
      (fun e -> Util.(e |> member "hostPrefix" |> to_string))
      (List.assoc_opt "smithy.api#endpoint" traits)
  in
  let fields =
    if input = unit_type then []
    else
      match IdMap.find input shapes with
      | Structure l, _ -> l
      | _ -> assert false
  in
  let params, fields' =
    List.partition
      (fun (_, _, traits) -> List.mem_assoc "smithy.api#httpQuery" traits)
      fields
  in
  let params =
    List.fold_left
      (fun rem ((nm, _, traits) as field) ->
        let optional = optional_member field in
        [%expr
          let params = [%e rem] in
          [%e
            let add_param =
              [%expr
                ( [%e
                    B.pexp_constant
                      (const_string
                         (Yojson.Safe.Util.to_string
                            (List.assoc "smithy.api#httpQuery" traits)))],
                  [
                    [%e
                      convert_to_string ~shapes ~rename ~fields nm
                        (ident (field_name nm ^ "'"))];
                  ] )
                :: params]
            in
            if optional then
              [%expr
                match [%e ident (field_name nm ^ "'")] with
                | Some [%p B.ppat_var (Location.mknoloc (field_name nm ^ "'"))]
                  ->
                    [%e add_param]
                | None -> params]
            else add_param]])
      [%expr []] params
  in
  let params', fields' =
    List.partition
      (fun (_, _, traits) -> List.mem_assoc "smithy.api#httpQueryParams" traits)
      fields'
  in
  let params =
    (*ZZZ*)
    List.fold_left
      (fun rem (nm, _, _) ->
        [%expr
          ignore [%e ident (field_name nm ^ "'")];
          [%e rem]])
      params params'
  in
  let headers, fields' =
    List.partition
      (fun (_, _, traits) -> List.mem_assoc "smithy.api#httpHeader" traits)
      fields'
  in
  let headers =
    List.fold_left
      (fun rem ((nm, _, traits) as field) ->
        let optional = optional_member field in
        [%expr
          let headers = [%e rem] in
          [%e
            let add_header =
              [%expr
                ( [%e
                    B.pexp_constant
                      (const_string
                         (Yojson.Safe.Util.to_string
                            (List.assoc "smithy.api#httpHeader" traits)))],
                  [%e
                    convert_to_string ~shapes ~rename ~fields nm
                      (ident (field_name nm ^ "'"))] )
                :: headers]
            in
            if optional then
              [%expr
                match [%e ident (field_name nm ^ "'")] with
                | Some [%p B.ppat_var (Location.mknoloc (field_name nm ^ "'"))]
                  ->
                    [%e add_header]
                | None -> headers]
            else add_header]])
      [%expr []] headers
  in
  let headers', fields' =
    List.partition
      (fun (_, _, traits) ->
        List.mem_assoc "smithy.api#httpHeaderPrefix" traits)
      fields'
  in
  let headers =
    (*ZZZ*)
    List.fold_left
      (fun rem (nm, _, _) ->
        [%expr
          ignore [%e ident (field_name nm ^ "'")];
          [%e rem]])
      headers headers'
  in
  let fields' =
    List.filter
      (fun (_, _, traits) -> not (List.mem_assoc "smithy.api#httpLabel" traits))
      fields'
  in
  Format.eprintf "ZZZZZ %d@." (List.length fields');
  (*ZZZ*)
  let builder =
    let add_content_type typ headers =
      [%expr
        ("Content-Type", [%e B.pexp_constant (const_string typ)])
        :: [%e headers]]
    in
    let body, headers =
      match fields' with
      | [] -> ([%expr None], headers)
      | [ ((nm, typ, traits) as field) ]
        when List.mem_assoc "smithy.api#httpPayload" traits ->
          let value = ident (field_name nm ^ "'") in
          let convert, headers' =
            match type_of_shape shapes typ with
            | Blob ->
                ( [%expr Some [%e value]],
                  add_content_type "application/octet-stream" [%expr headers] )
            | String ->
                ( [%expr Some [%e value]],
                  add_content_type "text/plain" [%expr headers] )
            | Enum _ ->
                ( [%expr
                    Some
                      (Yojson.Safe.Util.to_string
                         (To_JSON.([%e ident (type_name ~rename typ)])
                            [%e value]))],
                  add_content_type "text/plain" [%expr headers] )
            | Document ->
                ( [%expr Some (Yojson.Safe.to_string [%e value])],
                  add_content_type "application/json" [%expr headers] )
            | Union _ | Structure _ ->
                ( [%expr
                    Some
                      (Yojson.Safe.to_string
                         (let open To_JSON in
                          [%e
                            let path =
                              Longident.(Ldot (Lident "Converters", "To_JSON"))
                            in
                            converter ~rename ~path ~sparse:false typ]
                            [%e value]))],
                  add_content_type "application/json" [%expr headers] )
            | _ -> assert false
          in
          if optional_member field then
            ( [%expr
                match [%e value] with
                | None -> None
                | Some [%p B.ppat_var (Location.mknoloc (field_name nm ^ "'"))]
                  ->
                    [%e convert]],
              [%expr
                let headers = [%e headers] in
                match [%e value] with
                | None -> headers
                | Some [%p B.ppat_var (Location.mknoloc (field_name nm ^ "'"))]
                  ->
                    [%e headers']] )
          else
            ( convert,
              [%expr
                let headers = [%e headers] in
                [%e headers']] )
      | _ ->
          ( [%expr
              Some
                (Yojson.Safe.to_string
                   (let open! To_JSON in
                    [%e
                      structure_json_converter ~rename ~fixed_fields:false
                        fields']))],
            add_content_type "application/json" headers )
    in
    [%expr
      fun ~test:_ (*ZZZ idempotency token*) k ->
        [%e
          constructor_parameters ~shapes ~rename ~fields
            ~body:
              (List.fold_left
                 (fun expr (nm, _, _) ->
                   B.pexp_let Nonrecursive
                     [
                       B.value_binding
                         ~pat:
                           (B.ppat_var (Location.mknoloc (field_name nm ^ "'")))
                         ~expr:(ident (field_name nm));
                     ]
                     expr)
                 [%expr
                   k [%e body]
                     [%e
                       match host_prefix with
                       | Some prefix ->
                           [%expr
                             Some
                               [%e
                                 compile_pattern ~shapes ~rename ~fields prefix]]
                       | None -> [%expr None]]
                     [%e compile_pattern ~shapes ~rename ~fields path]
                     [%e params] [%e headers]]
                 fields)]]
  in
  let errors = service_info.errors @ errors in
  let errors =
    List.fold_right
      (fun name lst ->
        [%expr
          ( [%e B.pexp_constant (const_string name.identifier)],
            fun x ->
              [%e
                B.pexp_variant
                  (constr_name name.identifier)
                  (Some
                     [%expr
                       [%e
                         B.pexp_ident
                           (Location.mknoloc
                              (Longident.Ldot
                                 (Lident "From_JSON", type_name ~rename name)))]
                         x])] )
          :: [%e lst]])
      errors [%expr []]
  in
  B.pstr_value Nonrecursive
    [
      {
        (B.value_binding
           ~pat:(B.ppat_var (Location.mknoloc (type_name ~rename nm)))
           ~expr:
             [%expr
               fun () ->
                 Converters.create_rest_json_operation
                   ~method_:
                     [%e
                       B.pexp_construct
                         (Location.mknoloc
                            (Longident.Lident (constr_name meth)))
                         None]
                   ~builder:[%e builder]
                   ~parser:
                     [%e
                       B.pexp_ident
                         (Location.mknoloc
                            (if output.namespace = "smithy.api" then
                               Longident.(
                                 Ldot
                                   ( Ldot (Lident "Converters", "From_JSON"),
                                     type_name ~rename output ))
                             else
                               Longident.Ldot
                                 (Lident "From_JSON", type_name ~rename output)))]
                   ~errors:[%e errors]])
        with
        pvb_attributes = docs;
      };
    ]

let compile_json_operation ~service_id ~service_info ~protocol ~shapes ~rename
    nm { input; output; errors; _ } traits =
  let field_refs name m =
    if name.namespace = "smithy.api" then m
    else
      match IdMap.find name shapes with
      | Structure l, _ ->
          List.fold_left
            (fun m (nm, _, _) -> StringMap.add nm name.identifier m)
            m l
      | _ -> assert false
  in
  let docs =
    (if input.namespace = "smithy.api" then []
     else
       [
         doc_attr
           ("See type {!type:" ^ type_name ~rename input
          ^ "} for a description of the parameters");
       ])
    @ documentation ~shapes
        ~field_refs:(StringMap.empty |> field_refs output |> field_refs input)
        traits
  in
  (* Method: POST, uri: / *)
  let host_prefix =
    Option.map
      (fun e -> Util.(e |> member "hostPrefix" |> to_string))
      (List.assoc_opt "smithy.api#endpoint" traits)
  in
  let fields =
    if input = unit_type then []
    else
      match IdMap.find input shapes with
      | Structure l, _ -> l
      | _ -> assert false
  in
  let builder =
    [%expr
      fun ~test:_ (*ZZZ idempotency token*) k ->
        [%e
          constructor_parameters ~shapes ~rename ~fields
            ~body:
              (List.fold_left
                 (fun expr (nm, _, _) ->
                   B.pexp_let Nonrecursive
                     [
                       B.value_binding
                         ~pat:
                           (B.ppat_var (Location.mknoloc (field_name nm ^ "'")))
                         ~expr:(ident (field_name nm));
                     ]
                     expr)
                 [%expr
                   k
                     (let open! To_JSON in
                      [%e
                        structure_json_converter ~rename ~fixed_fields:true
                          (*ZZZ*) fields])
                     [%e
                       match host_prefix with
                       | Some prefix ->
                           [%expr
                             Some
                               [%e
                                 compile_pattern ~shapes ~rename ~fields prefix]]
                       | None -> [%expr None]]]
                 fields)]]
  in
  let errors = service_info.errors @ errors in
  let errors =
    List.fold_right
      (fun name lst ->
        [%expr
          ( [%e B.pexp_constant (const_string name.identifier)],
            fun x ->
              [%e
                B.pexp_variant
                  (constr_name name.identifier)
                  (Some
                     [%expr
                       [%e
                         B.pexp_ident
                           (Location.mknoloc
                              (Longident.Ldot
                                 (Lident "From_JSON", type_name ~rename name)))]
                         x])] )
          :: [%e lst]])
      errors [%expr []]
  in
  B.pstr_value Nonrecursive
    [
      {
        (B.value_binding
           ~pat:(B.ppat_var (Location.mknoloc (type_name ~rename nm)))
           ~expr:
             [%expr
               fun () ->
                 Converters.create_JSON_operation
                   ~variant:
                     [%e
                       match protocol with
                       | `AwsJson1_0 -> B.pexp_variant "AwsJson1_0" None
                       | `AwsJson1_1 -> B.pexp_variant "AwsJson1_1" None]
                   ~target:
                     [%e
                       B.pexp_constant
                         (const_string
                            (service_id.identifier ^ "." ^ nm.identifier))]
                   ~builder:[%e builder]
                   ~parser:
                     [%e
                       B.pexp_ident
                         (Location.mknoloc
                            (if output.namespace = "smithy.api" then
                               Longident.(
                                 Ldot
                                   ( Ldot (Lident "Converters", "From_JSON"),
                                     type_name ~rename output ))
                             else
                               Longident.Ldot
                                 (Lident "From_JSON", type_name ~rename output)))]
                   ~errors:[%e errors]])
        with
        pvb_attributes = docs;
      };
    ]

let compile_operation ~service_id ~service_info ~protocol ~shapes ~rename nm
    ({ input; output; http_request_tests; http_response_tests; _ } as info)
    traits =
  (match protocol with
  | (`AwsJson1_1 | `AwsJson1_0) as protocol ->
      compile_json_operation ~service_id ~service_info ~protocol ~shapes ~rename
        nm info traits
  | `RestJson1 ->
      compile_rest_json_operation ~service_info ~shapes ~rename nm info traits)
  ::
  (let has_optionals =
     let fields =
       if input = unit_type then []
       else
         match IdMap.find input shapes with
         | Structure l, _ -> l
         | _ -> assert false
     in
     structure_has_optionals fields || fields = []
   in
   (*ZZZ tests associated to errors *)
   List.map
     (compile_http_request_tests ~shapes ~rename ~input has_optionals nm)
     (List.filter
        (fun (test : http_request_test) ->
          match test.applies_to with
          | None | Some `Client -> true
          | Some `Server -> false)
        http_request_tests)
   @ List.map
       (compile_http_response_tests ~shapes ~rename ~output nm)
       (List.filter
          (fun (test : http_response_test) ->
            match test.applies_to with
            | None | Some `Client -> true
            | Some `Server -> false)
          http_response_tests))

let compile_operations ~service_id ~service_info ~protocol ~shapes ~rename =
  let compile shs =
    List.fold_right
      (fun nm rem ->
        let ty, traits = IdMap.find nm shapes in
        match ty with
        | Operation info ->
            (match protocol with
            | (`AwsJson1_1 | `AwsJson1_0 | `RestJson1) as protocol ->
                compile_operation ~service_id ~service_info ~protocol ~shapes
                  ~rename nm info traits
            | _ -> [])
            @ rem
        | _ -> assert false)
      shs []
  in
  let rec compile_resources l =
    List.concat
    @@ List.fold_right
         (fun nm ops ->
           let typ, traits = IdMap.find nm shapes in
           match typ with
           | Resource r ->
               let name =
                 nm.identifier |> to_snake_case
                 |> Re.replace_string Re.(compile (char '_')) ~by:" "
                 |> String.capitalize_ascii
                 |> fun s ->
                 if String.ends_with ~suffix:" resource" s then
                   String.sub s 0 (String.length s - 9)
                 else s
               in
               let add x l =
                 match x with
                 | None -> l
                 | Some x -> x :: List.filter (fun y -> x <> y) l
               in
               let operations =
                 r.operations @ r.collection_operations
                 |> add r.list |> add r.delete |> add r.update |> add r.read
                 |> add r.put |> add r.create
               in
               (B.pstr_attribute (text_attr ("{2 " ^ name ^ "}"))
                :: toplevel_documentation ~shapes traits
               @ compile operations
               @ compile_resources r.resources)
               :: ops
           | _ -> assert false)
         l []
  in
  compile service_info.operations @ compile_resources service_info.resources

module Rules = struct
  type expr =
    | Bool of bool
    | String of string
    | Int of int
    | Apply of { f : string; args : expr list; assign : string option }
    | Var of string

  type rule = { conditions : expr list; desc : rule_desc }

  and rule_desc =
    | Tree of rule list
    | Error of string
    | Endpoint of {
        url : expr;
        auth_schemes : auth_schemes list option;
        headers : (string * string) list;
      }

  and auth_schemes = {
    name : string;
    signing_region : string option;
    signing_region_set : string list option;
    signing_name : string;
    disable_double_encoding : bool option;
  }

  let array_access_re =
    Re.(
      compile
        (whole_string
           (seq [ group (rep any); char '['; group (rep any); char ']' ])))

  let compile_string s =
    compile_string s (fun label property greedy ->
        assert (not greedy);
        let t = uncapitalized_identifier label in
        let t =
          if t = "endpoint" then [%expr Uri.to_string [%e ident t]] else ident t
        in
        match property with
        | None -> t
        | Some f ->
            B.pexp_field t
              (Location.mknoloc
                 Longident.(
                   Ldot
                     ( Ldot (Lident "Converters", "Endpoint"),
                       uncapitalized_identifier f ))))

  let rec compile_expr e =
    match e with
    | Bool b -> if b then [%expr true] else [%expr false]
    | String s -> compile_string s
    | Int i -> B.pexp_constant (Pconst_integer (Int.to_string i, None))
    | Var nm -> ident (uncapitalized_identifier nm)
    | Apply { f; args; _ } -> (
        let fn f =
          B.pexp_ident
            (Location.mknoloc
               Longident.(Ldot (Ldot (Lident "Converters", "Endpoint"), f)))
        in
        match (f, args) with
        | "aws.isVirtualHostableS3Bucket", [ b; b' ] ->
            [%expr
              [%e fn "is_virtual_hostable_s3_bucket"]
                [%e compile_expr b] [%e compile_expr b']]
        | "aws.parseArn", [ b ] ->
            [%expr [%e fn "parse_arn"] [%e compile_expr b]]
        | "aws.partition", [ r ] ->
            [%expr [%e fn "partition"] [%e compile_expr r]]
        | "booleanEquals", ([ e; Bool true ] | [ Bool true; e ]) ->
            compile_expr e
        | "booleanEquals", [ e; Bool false ] -> [%expr not [%e compile_expr e]]
        | "booleanEquals", [ e; e' ] ->
            [%expr [%e compile_expr e] = [%e compile_expr e']]
        | "getAttr", [ r; String s ] -> (
            match Re.exec_opt array_access_re s with
            | Some g ->
                [%expr
                  [%e fn "array_get_opt"]
                    [%e
                      B.pexp_field (compile_expr r)
                        (Location.mknoloc
                           Longident.(
                             Ldot
                               ( Ldot (Lident "Converters", "Endpoint"),
                                 uncapitalized_identifier (Re.Group.get g 1) )))]
                    [%e
                      B.pexp_constant (Pconst_integer (Re.Group.get g 2, None))]]
            | None ->
                B.pexp_field (compile_expr r)
                  (Location.mknoloc
                     Longident.(
                       Ldot
                         ( Ldot (Lident "Converters", "Endpoint"),
                           uncapitalized_identifier s ))))
        | "isSet", [ e ] -> [%expr [%e compile_expr e] <> None]
        | "isValidHostLabel", [ e; e' ] ->
            [%expr
              [%e fn "is_valid_host_label"] [%e compile_expr e]
                [%e compile_expr e']]
        | "not", [ Apply { f = "isSet"; args = [ e ]; _ } ] ->
            [%expr [%e compile_expr e] = None]
        | "not", [ e ] -> [%expr not [%e compile_expr e]]
        | "parseURL", [ e ] -> [%expr [%e fn "parse_url"] [%e compile_expr e]]
        | "stringEquals", [ e; e' ] ->
            [%expr [%e compile_expr e] = [%e compile_expr e']]
        | "substring", [ s; i; j; rev ] ->
            [%expr
              [%e fn "substring"] [%e compile_expr s] [%e compile_expr i]
                [%e compile_expr j] [%e compile_expr rev]]
        | "uriEncode", [ e ] ->
            [%expr
              Some (Uri.pct_encode ~component:`Generic [%e compile_expr e])]
        | _ ->
            Format.eprintf "%s@." f;
            assert false)

  let rec compile_rule rule =
    let body =
      match rule.desc with
      | Tree l -> compile_rule_list l
      | Error s -> [%expr Some (Result.error [%e compile_string s])]
      | Endpoint { url; headers; _ } ->
          [%expr
            Some
              (Result.ok
                 ( [%e
                     match url with
                     | Var "Endpoint" -> [%expr Uri.to_string endpoint]
                     | _ -> compile_expr url],
                   [%e
                     to_caml_list
                       (List.map
                          (fun (k, v) ->
                            [%expr
                              [%e B.pexp_constant (const_string k)],
                                [%e compile_string v]])
                          headers)] ))]
      (*ZZZ*)
    in
    List.fold_right
      (fun expr rem ->
        let e = compile_expr expr in
        match expr with
        | Apply { assign = Some x; _ } ->
            [%expr
              let* [%p
                     B.ppat_var (Location.mknoloc (uncapitalized_identifier x))]
                  =
                [%e e]
              in
              [%e rem]]
        | Apply { f = "isSet"; args = [ Var x ]; _ } ->
            let x = uncapitalized_identifier x in
            [%expr
              let* [%p B.ppat_var (Location.mknoloc x)] = [%e ident x] in
              [%e rem]]
        | Apply { f = "aws.parseArn"; _ } ->
            [%expr
              let* _ = [%e e] in
              [%e rem]]
        | _ ->
            [%expr
              let* () = check [%e e] in
              [%e rem]])
      rule.conditions body

  and compile_rule_list rules =
    match rules with
    | [ rule ] -> compile_rule rule
    | _ ->
        [%expr
          seq
            [%e
              List.fold_right
                (fun rule rem ->
                  [%expr (fun () -> [%e compile_rule rule]) :: [%e rem]])
                rules [%expr []]]]

  let handle_endpoint ruleset =
    let rec expr c =
      match c with
      | `Bool _ -> Bool Util.(to_bool c)
      | `String _ -> String Util.(to_string c)
      | `Int _ -> Int Util.(to_int c)
      | `Assoc _ ->
          if Util.member "fn" c <> `Null then
            let f = Util.(c |> member "fn" |> to_string) in
            (*
      2 aws.isVirtualHostableS3Bucket
        ====> packages/util-endpoints/src/lib/aws/isVirtualHostableS3Bucket.ts
      7 aws.parseArn
    356 aws.partition
        ====> packages/util-endpoints/src/lib/aws/partition.ts
   3978 booleanEquals
   1624 getAttr
    518 isSet
     31 isValidHostLabel
    167 not
    279 parseURL
    426 stringEquals
      5 substring
      2 uriEncode
*)
            let args = Util.(c |> member "argv" |> to_list |> List.map expr) in
            let assign = Util.(c |> member "assign" |> to_option to_string) in
            Apply { f; args; assign }
          else if Util.member "ref" c <> `Null then (
            assert (Util.member "assert" c = `Null);
            Var Util.(c |> member "ref" |> to_string))
          else assert false
      | _ -> assert false
    in
    let auth_schemes s =
      {
        name = Util.(s |> member "name" |> to_string);
        signing_region =
          Util.(s |> member "signingRegion" |> to_option to_string);
        signing_region_set =
          Util.(
            s |> member "signingRegionSet" |> to_option to_list
            |> Option.map (List.map to_string));
        signing_name = Util.(s |> member "signingName" |> to_string);
        disable_double_encoding =
          Util.(s |> member "disableDoubleEncoding" |> to_option to_bool);
      }
    in
    let rec rule_list r = List.map rule Util.(r |> member "rules" |> to_list)
    and rule r =
      let typ = Util.(r |> member "type" |> to_string) in
      {
        conditions = Util.(r |> member "conditions" |> to_list |> List.map expr);
        desc =
          (match typ with
          | "tree" -> Tree (rule_list r)
          | "endpoint" ->
              let e = Util.(r |> member "endpoint") in
              Endpoint
                {
                  url = Util.(e |> member "url" |> expr);
                  headers =
                    Util.(
                      e |> member "headers" |> to_assoc
                      |> List.map (fun (nm, value) ->
                             ( nm,
                               value |> to_list |> fun l ->
                               match l with
                               | [ s ] -> to_string s
                               | _ -> assert false )));
                  auth_schemes =
                    Util.(
                      e |> member "properties" |> member "authSchemes"
                      |> to_option to_list
                      |> Option.map (List.map auth_schemes));
                }
          | "error" -> Error Util.(r |> member "error" |> to_string)
          | _ -> assert false);
      }
    in
    let parameters =
      Util.(
        ruleset |> member "parameters" |> to_assoc
        |> List.map (fun (nm, info) ->
               ( nm,
                 info |> member "required" |> to_bool,
                 info |> member "default" |> to_option to_bool )))
    in
    let rules = rule_list ruleset in
    List.fold_right
      (fun (p, required, default) rem ->
        let p = uncapitalized_identifier p in
        B.pexp_fun
          (if (*p = "region" ||*) required && default = None then Labelled p
           else Optional p)
          (Option.map
             (fun b -> if b then [%expr true] else [%expr false])
             default)
          (B.ppat_var (Location.mknoloc p))
          rem)
      parameters
      [%expr
        fun () ->
          let open Converters.Endpoint.Rules in
          [%e compile_rule_list rules]]
end

let protocols =
  [
    ("aws.protocols#awsJson1_0", `AwsJson1_0);
    ("aws.protocols#awsJson1_1", `AwsJson1_1);
    ("aws.protocols#restJson1", `RestJson1);
    ("aws.protocols#restXml", `RestXml);
    ("aws.protocols#awsQuery", `AwsQuery);
    ("aws.protocols#ec2Query", `Ec2Query);
  ]

let compile_service shs service =
  let name =
    let sdk_id =
      let _, (_, traits) = service in
      traits
      |> List.assoc "aws.api#service"
      |> Yojson.Safe.Util.member "sdkId"
      |> Yojson.Safe.Util.to_string
    in
    let space_re = Re.(compile (set " -")) in
    sdk_id |> String.lowercase_ascii |> Re.replace space_re ~f:(fun _ -> "_")
  in
  let _, protocol =
    let _, (_, traits) = service in
    List.find (fun (name, _) -> List.mem_assoc name traits) protocols
  in
  let service_info =
    match service with _, (Service info, _) -> info | _ -> assert false
  in
  let rename = service_info.rename in
  let ch = open_out (Filename.concat "generated" (name ^ ".ml")) in
  let f = Format.formatter_of_out_channel ch in
  let inputs, outputs, operations = compute_inputs_outputs shs service in
  let types = print_types ~rename ~inputs ~outputs ~operations shs in
  let input_shapes = IdMap.filter (fun id _ -> IdSet.mem id inputs) shs in
  let record_constructors = print_constructors ~shapes:input_shapes ~rename in
  let converters =
    match protocol with
    | `AwsJson1_0 | `AwsJson1_1 ->
        print_to_json ~rename ~fixed_fields:true input_shapes
    | `RestJson1 -> print_to_json ~rename ~fixed_fields:false input_shapes
    | `RestXml -> print_to_xml ~rename input_shapes
    | (`AwsQuery | `Ec2Query) as protocol ->
        print_to_graph ~rename ~protocol input_shapes
  in
  let output_shapes = IdMap.filter (fun id _ -> IdSet.mem id outputs) shs in
  let converters' =
    match protocol with
    | `AwsJson1_0 | `AwsJson1_1 ->
        print_from_json ~rename ~fixed_fields:true output_shapes
    | `RestJson1 -> print_from_json ~rename ~fixed_fields:false output_shapes
    | `RestXml | `AwsQuery | `Ec2Query -> print_from_xml ~rename output_shapes
  in
  let toplevel_doc =
    let _, (_, traits) = service in
    toplevel_documentation ~shapes:shs traits
  in
  let endpoint =
    match List.assoc_opt "smithy.rules#endpointRuleSet" (snd (snd service)) with
    | Some ruleset ->
        [
          B.pstr_value Nonrecursive
            [
              B.value_binding
                ~pat:(B.ppat_var (Location.mknoloc "aws_endpoint"))
                ~expr:(Rules.handle_endpoint ruleset);
            ];
        ]
    | None -> []
  in
  let operations =
    compile_operations ~service_id:(fst service) ~service_info ~protocol
      ~shapes:shs ~rename
  in
  let toggle_hide = [ B.pstr_attribute (text_attr "/*") ] in
  Format.fprintf f "%a@." Ppxlib.Pprintast.structure
    (toplevel_doc
    @ B.pstr_attribute (text_attr "{1 Type definitions}")
      :: types
      ::
      (if record_constructors = [] then []
       else [ B.pstr_attribute (text_attr "{1 Record constructors}") ])
    @ record_constructors @ toggle_hide @ endpoint @ converters @ converters'
    @ toggle_hide
    @ (B.pstr_attribute (text_attr "{1 Operations}") :: operations));
  close_out ch

let compile dir f =
  let shs = parse (Filename.concat dir f) in
  IdMap.iter
    (fun nm info -> compile_service shs (nm, info))
    (IdMap.filter
       (fun _ (typ, _) -> match typ with Service _ -> true | _ -> false)
       shs)

let () =
  let _f { namespace = _; identifier = _ } = () in
  let dir = "/home/jerome/sources/aws-sdk-rust/aws-models" in
  (*
  let dir = "." in
  *)
  if true then
    let files = Array.to_list (Sys.readdir dir) in
    let files =
      List.filter
        (fun f ->
          f <> "sdk-default-configuration.json"
          && f <> "sdk-endpoints.json" && f <> "sdk-partitions.json"
          && Filename.check_suffix f ".json")
        files
    in
    List.iter (fun f -> compile dir f) files
  else
    let f = "s3.json" in
    compile dir f

(*
Aws_lwt.perform S3.list_buckets
Aws_lwt.paginate S3.list_buckets  ===> stream of responses

type request = {
  method_ : [ `GET ];
  host : string;
  path : string;
  params : string list;
  headers : (string * string) list;
  body : string;
}

type response = { code : int; headers : (string * string) list; body : string }

type ('perform, 'result, 'response) operation = {
  build_request : (request -> 'result) -> 'perform;
  parse_response : response -> 'response;
}

let build_request k ~x ~y = k (x ^ y)
let parse_response x = (x, x)
(*let op = { build_request; parse_response }*)

let perform ~(f : request -> response Lwt.t)
    (op : ('perform, 'response Lwt.t, 'response) operation) : 'perform =
  op.build_request @@ fun request ->
  Lwt.bind (f request) @@ fun response ->
  Lwt.return (op.parse_response response)

type ('response, 'result) operation' =
  | F of ((response -> 'response) -> request -> 'result)

let perform f =
  F
    (fun parse_response request ->
      Lwt.bind (f request) @@ fun response ->
      Lwt.return (parse_response response))

let list_foo parse (F perform) x = perform parse x

 ('response, paginate, 'result) t -> request_params -> 'result




   val stream_out : ('response, output_stream, string Lwt_stream.t) operation

   val paginate : ('response, paginate, 'response Lwt_stream.t) operation
   val perform : ('response, paginate, 'response Lwt.t) operation
*)
