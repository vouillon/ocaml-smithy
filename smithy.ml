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

type shape =
  | Blob (* string *)
  | Boolean
  | String (* string *)
  | Enum of (string * shape_id * traits) list
  | Integer (* int32 *)
  | Long (* int64 *)
  | Float
  | Double
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

and operation = { input : shape_id; output : shape_id; errors : shape_id list }

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
                     ( value,
                       { namespace = "smithy.api"; identifier = "Unit" },
                       [] )))
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
              resources = parse_list " resources";
            }
      | "operation" ->
          Operation
            {
              input = fst (parse_member "input");
              output = fst (parse_member "output");
              errors = parse_list "errors";
            }
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

let type_name id =
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
      | _ ->
          prerr_endline id.identifier;
          assert false)
  | _ -> uncapitalized_identifier id.identifier

let field_name = uncapitalized_identifier
let constr_name = capitalized_identifier

let optional_member (_, _, traits) =
  not
    (List.mem_assoc "smithy.api#required" traits
    || List.mem_assoc "smithy.api#default" traits)

let flattened_member (_, _, traits) =
  List.mem_assoc "smithy.api#xmlFlattened" traits

let type_of_shape shapes nm =
  if nm.namespace = "smithy.api" then (
    match nm.identifier with
    | "PrimitiveLong" -> Long
    | _ ->
        Format.eprintf "%s/%s@." nm.namespace nm.identifier;
        assert false)
  else
    match IdMap.find_opt nm shapes with
    | None -> assert false
    | Some (typ, _) -> typ

open Ast_helper

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

let documentation ~shapes ?(field_refs = StringMap.empty) traits =
  match List.assoc_opt "smithy.api#documentation" traits with
  | None -> None
  | Some doc -> (
      let doc = Yojson.Safe.Util.to_string doc in
      match documentation ~shapes ~field_refs doc with
      | Some doc -> Some (Docstrings.docstring doc loc)
      | None -> None)

let type_ident id =
  Typ.constr (Location.mknoloc (Longident.Lident (type_name id))) []

let default_value shapes typ default =
  match default with
  | `String s -> (
      match type_of_shape shapes typ with
      | String -> Exp.constant (Const.string s)
      | Enum _ ->
          [%expr
            ([%e
               Exp.construct
                 (Location.mknoloc (Longident.Lident (constr_name s)))
                 None]
              : [%t type_ident typ])]
      | _ -> assert false)
  | `Int n ->
      Exp.constant
        (match type_of_shape shapes typ with
        | Float | Double -> Const.float (Printf.sprintf "%d." n)
        | Integer -> Const.int n
        | Long -> Const.int64 (Int64.of_int n)
        | _ -> assert false)
  | `Bool b -> if b then [%expr true] else [%expr false]
  | _ -> assert false

let constructor_parameters ~shapes ~fields ~body =
  let has_optionals =
    List.exists
      (fun (_, _, traits) ->
        List.mem_assoc "smithy.api#default" traits
        || not (List.mem_assoc "smithy.api#required" traits))
      fields
  in
  List.fold_right
    (fun field (expr : Parsetree.expression) ->
      let nm, typ, traits' = field in
      match List.assoc_opt "smithy.api#default" traits' with
      | Some default when default <> `Null ->
          Exp.fun_
            (Optional (field_name nm))
            (Some (default_value shapes typ default))
            (Pat.var (Location.mknoloc (field_name nm)))
            expr
      | _ ->
          let optional = optional_member field in
          Exp.fun_
            (if optional then Optional (field_name nm)
            else Labelled (field_name nm))
            None
            (Pat.var (Location.mknoloc (field_name nm)))
            expr)
    fields
    (if has_optionals || fields = [] then Exp.fun_ Nolabel None [%pat? ()] body
    else body)

let print_constructor shapes name fields =
  let body =
    Exp.constraint_
      (Exp.record
         (List.map
            (fun (nm, _, _) ->
              let label = Location.mknoloc (Longident.Lident (field_name nm)) in
              (label, Exp.ident label))
            fields)
         None)
      (type_ident name)
  in
  let expr = constructor_parameters ~shapes ~fields ~body in
  [%stri let [%p Pat.var (Location.mknoloc (type_name name))] = [%e expr]]

let print_constructors shapes =
  IdMap.fold
    (fun name (sh, _) rem ->
      match sh with
      | Structure l when l <> [] -> print_constructor shapes name l :: rem
      | _ -> rem)
    shapes []

let type_constructor ?info ?arg_type nm =
  Type.constructor ?info
    ?args:
      (Option.map
         (fun typ -> Parsetree.Pcstr_tuple [ type_ident typ ])
         arg_type)
    (Location.mknoloc (constr_name nm))

let print_type ?heading ~inputs ~shapes (nm, (sh, traits)) =
  let text = Option.map (fun h -> [ Docstrings.docstring h loc ]) heading in
  let docs =
    Some
      {
        Docstrings.docs_pre = documentation ~shapes traits;
        docs_post =
          (match sh with
          | Structure l when l <> [] && IdSet.mem nm inputs ->
              Some
                (Docstrings.docstring
                   ("See associated record builder function {!val:"
                  ^ type_name nm ^ "}.")
                   loc)
          | _ -> None);
      }
  in

  let manifest_type manifest =
    Type.mk ?text ?docs ~manifest (Location.mknoloc (type_name nm))
  in
  match sh with
  | Blob -> manifest_type [%type: string]
  | Boolean -> manifest_type [%type: bool]
  | String -> manifest_type [%type: string]
  | Enum l ->
      let l =
        List.map
          (fun (nm, _, traits) ->
            type_constructor ~info:(documentation ~shapes traits) nm)
          l
      in
      Type.mk ?docs ~kind:(Ptype_variant l) (Location.mknoloc (type_name nm))
  | Integer -> manifest_type [%type: int]
  | Long -> manifest_type [%type: Int64.t]
  | Float | Double -> manifest_type [%type: float]
  | Timestamp -> manifest_type [%type: CalendarLib.Calendar.t]
  | Document -> manifest_type [%type: Yojson.Safe.t]
  | List (id, _) ->
      let sparse = List.mem_assoc "smithy.api#sparse" traits in
      let id = type_ident id in
      manifest_type
        [%type: [%t if sparse then [%type: [%t id] option] else id] list]
  | Map ((key, _), (value, _)) ->
      let sparse = List.mem_assoc "smithy.api#sparse" traits in
      let id = type_ident value in
      manifest_type
        [%type:
          ( [%t type_ident key],
            [%t if sparse then [%type: [%t id] option] else id] )
          Converters.map]
  | Structure [] -> manifest_type [%type: unit]
  | Structure l ->
      let l =
        List.map
          (fun ((nm, typ, traits) as field) ->
            let optional = optional_member field in
            let id = type_ident typ in
            Type.field
              ~info:(documentation ~shapes traits)
              (Location.mknoloc (field_name nm))
              (if optional then [%type: [%t id] option] else id))
          l
      in
      Type.mk ?text ?docs ~kind:(Ptype_record l)
        (Location.mknoloc (type_name nm))
  | Union l ->
      let l =
        List.map
          (fun (nm, typ, _) ->
            assert (typ <> { namespace = "smithy.api"; identifier = "Unit" });
            type_constructor
              ~info:(documentation ~shapes traits)
              ~arg_type:typ nm)
          l
      in
      Type.mk ?docs ~kind:(Ptype_variant l) (Location.mknoloc (type_name nm))
  | Service _ | Resource _ | Operation _ -> assert false

let print_types ~inputs ~operations shapes =
  let types =
    IdMap.bindings
      (IdMap.filter
         (fun _ (typ, _) ->
           match typ with
           | Service _ | Operation _ | Resource _ -> false
           | _ -> true)
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
        print_type ?heading ~inputs ~shapes err)
      types
  in
  Str.type_ Recursive
    (print_types "{2 Operations}" operations
    @ print_types "{2 Additional types}" types
    @ print_types "{2 Errors}" errors)

let ident id = Exp.ident (Location.mknoloc (Longident.Lident id))

let converter ~path ~sparse id =
  let convert id =
    match id.namespace with
    | "smithy.api" ->
        Exp.ident
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
                  | _ -> assert false )))
    | _ -> ident (type_name id)
  in
  if sparse then
    [%expr
      [%e Exp.ident (Location.mknoloc (Longident.Ldot (path, "option")))]
        [%e convert id]]
  else convert id

let member_name ?(name = "smithy.api#jsonName") (nm, _, traits) =
  try Yojson.Safe.Util.to_string (List.assoc name traits) with Not_found -> nm

let pat_construct nm =
  Pat.construct (Location.mknoloc (Longident.Lident (constr_name nm)))

let structure_json_converter fields =
  let path = Longident.(Ldot (Lident "Converters", "To_JSON")) in
  [%expr
    Converters.To_JSON.structure
      [%e
        List.fold_right
          (fun ((nm, typ, _) as field) lst ->
            let optional = optional_member field in
            [%expr
              ( [%e Exp.constant (Const.string (member_name field))],
                [%e converter ~path ~sparse:optional typ]
                  [%e ident (field_name nm ^ "'")] )
              :: [%e lst]])
          fields [%expr []]]]

let to_json (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "To_JSON")) in
  let nm = type_name name ^ "'" in
  Vb.mk
    (Pat.var (Location.mknoloc (type_name name)))
    (Exp.fun_ Nolabel None
       [%pat? ([%p Pat.var (Location.mknoloc nm)] : [%t type_ident name])]
       (Exp.constraint_
          (match sh with
          | Blob -> [%expr Converters.To_JSON.blob [%e ident nm]]
          | Boolean -> [%expr Converters.To_JSON.boolean [%e ident nm]]
          | String -> [%expr Converters.To_JSON.string [%e ident nm]]
          | Enum l ->
              [%expr
                Converters.To_JSON.string
                  [%e
                    Exp.match_ (ident nm)
                      (List.map
                         (fun ((nm, _, _) as enum) ->
                           Exp.case (pat_construct nm None)
                             (Exp.constant
                                (Const.string
                                   (member_name ~name:"smithy.api#enumValue"
                                      enum))))
                         l)]]
          | Integer -> [%expr Converters.To_JSON.integer [%e ident nm]]
          | Long -> [%expr Converters.To_JSON.long [%e ident nm]]
          | Float | Double -> [%expr Converters.To_JSON.float [%e ident nm]]
          | Timestamp -> [%expr Converters.To_JSON.timestamp [%e ident nm]]
          | Document -> [%expr Converters.To_JSON.document [%e ident nm]]
          | List (id, _) ->
              let sparse = List.mem_assoc "smithy.api#sparse" traits in
              [%expr
                Converters.To_JSON.list [%e converter ~path ~sparse id]
                  [%e ident nm]]
          | Map (_key, (value, _)) ->
              let sparse = List.mem_assoc "smithy.api#sparse" traits in
              [%expr
                Converters.To_JSON.map
                  [%e converter ~path ~sparse value]
                  [%e ident nm]]
          | Structure [] -> [%expr Converters.To_JSON.structure []]
          | Structure l ->
              Exp.match_ (ident nm)
                [
                  Exp.case
                    (Pat.record
                       (List.map
                          (fun (nm, _, _) ->
                            ( Location.mknoloc (Longident.Lident (field_name nm)),
                              Pat.var (Location.mknoloc (field_name nm ^ "'"))
                            ))
                          l)
                       Closed)
                    (structure_json_converter l);
                ]
          | Union l ->
              [%expr
                Converters.To_JSON.structure
                  [
                    [%e
                      Exp.match_ (ident nm)
                        (List.map
                           (fun ((nm, typ, _) as constr) ->
                             Exp.case
                               (pat_construct nm (Some ([], [%pat? x])))
                               [%expr
                                 [%e
                                   Exp.constant
                                     (Const.string (member_name constr))],
                                   [%e (converter ~path ~sparse:false) typ] x])
                           l)];
                  ]]
          | Service _ | Resource _ | Operation _ -> assert false)
          [%type: Yojson.Safe.t]))

let print_to_json shs =
  [%str
    module To_JSON = struct
      [%%i Str.value Recursive (List.map to_json (IdMap.bindings shs))]
    end]

let field_xml_converter ?param ((nm, typ, _) as field) =
  let path = Longident.(Ldot (Lident "Converters", "To_JSON")) in
  let optional = param = None && optional_member field in
  let name =
    Exp.constant (Const.string (member_name ~name:"smithy.api#xmlName" field))
  in
  let flat = flattened_member field in

  let conv = converter ~path ~sparse:false typ in
  let id = ident (Option.value ~default:(field_name nm ^ "'") param) in
  let field_builder =
    if flat then [%expr [%e conv] ~flat:[%e name]]
    else [%expr Converters.To_XML.field [%e name] [%e conv]]
  in
  if optional then [%expr Converters.To_XML.option [%e field_builder] [%e id]]
  else [%expr [%e field_builder] [%e id]]

let structure_xml_converter fields =
  [%expr
    Converters.To_XML.structure
      [%e
        List.fold_right
          (fun field lst -> [%expr [%e field_xml_converter field] :: [%e lst]])
          fields [%expr []]]]

let to_xml (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "To_XML")) in
  let nm = type_name name ^ "'" in
  let wrap e =
    match sh with List _ | Map _ -> [%expr fun ?flat -> [%e e]] | _ -> e
  in
  Vb.mk
    (Pat.var (Location.mknoloc (type_name name)))
    (wrap
       (Exp.fun_ Nolabel None
          [%pat? ([%p Pat.var (Location.mknoloc nm)] : [%t type_ident name])]
          (Exp.constraint_
             (match sh with
             | Blob -> [%expr Converters.To_XML.blob [%e ident nm]]
             | Boolean -> [%expr Converters.To_XML.boolean [%e ident nm]]
             | String -> [%expr Converters.To_XML.string [%e ident nm]]
             | Enum l ->
                 [%expr
                   Converters.To_XML.string
                     [%e
                       Exp.match_ (ident nm)
                         (List.map
                            (fun ((nm, _, _) as enum) ->
                              Exp.case (pat_construct nm None)
                                (Exp.constant
                                   (Const.string
                                      (member_name ~name:"smithy.api#enumValue"
                                         enum))))
                            l)]]
             | Integer -> [%expr Converters.To_XML.integer [%e ident nm]]
             | Long -> [%expr Converters.To_XML.long [%e ident nm]]
             | Float | Double -> [%expr Converters.To_XML.float [%e ident nm]]
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
                         [%e e] ~name:[%e Exp.constant (Const.string name)]] )
                 |> fun e ->
                 [%expr
                   [%e e] [%e converter ~path ~sparse id] ?flat [%e ident nm]]
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
                           [%e e] ~key_name:[%e Exp.constant (Const.string nm)]]
                   )
                 |> fun e ->
                   match value_name with
                   | None -> e
                   | Some nm ->
                       [%expr
                         [%e e] ~value_name:[%e Exp.constant (Const.string nm)]]
                 )
                 |> fun e ->
                 [%expr
                   [%e e] [%e converter ~path ~sparse value] ?flat [%e ident nm]]
             | Structure [] -> [%expr Converters.To_XML.structure []]
             | Structure l ->
                 Exp.match_ (ident nm)
                   [
                     Exp.case
                       (Pat.record
                          (List.map
                             (fun (nm, _, _) ->
                               ( Location.mknoloc
                                   (Longident.Lident (field_name nm)),
                                 Pat.var
                                   (Location.mknoloc (field_name nm ^ "'")) ))
                             l)
                          Closed)
                       (structure_xml_converter l);
                   ]
             | Union l ->
                 [%expr
                   Converters.To_XML.structure
                     [
                       [%e
                         Exp.match_ (ident nm)
                           (List.map
                              (fun ((nm, _, _) as constr) ->
                                Exp.case
                                  (pat_construct nm (Some ([], [%pat? x])))
                                  (field_xml_converter ~param:"x" constr))
                              l)];
                     ]]
             | Service _ | Resource _ | Operation _ -> assert false)
             [%type: Converters.To_XML.t])))

let print_to_xml shs =
  [%str
    module To_XML = struct
      [%%i Str.value Recursive (List.map to_xml (IdMap.bindings shs))]
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

let field_graph_converter ~protocol ?param ((nm, typ, traits) as field) =
  let path = Longident.(Ldot (Lident "Converters", "To_JSON")) in
  let optional = param = None && optional_member field in
  let name =
    let name = Option.value ~default:nm (query_alt_name ~protocol ~traits) in
    assert (protocol <> `Ec2Query || String.capitalize_ascii name = name);
    Exp.constant (Const.string name)
  in
  let flat = flattened_member field in

  let conv = converter ~path ~sparse:false typ in
  let id = ident (Option.value ~default:(field_name nm ^ "'") param) in
  let field_builder =
    [%expr
      Converters.To_Graph.field [%e name]
        [%e if flat then [%expr [%e conv] ~flat:true] else [%expr [%e conv]]]]
  in
  if optional then [%expr Converters.To_Graph.option [%e field_builder] [%e id]]
  else [%expr [%e field_builder] [%e id]]

let structure_graph_converter ~protocol fields =
  [%expr
    Converters.To_Graph.structure
      [%e
        List.fold_right
          (fun field lst ->
            [%expr [%e field_graph_converter ~protocol field] :: [%e lst]])
          fields [%expr []]]]

let to_graph ~protocol (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "To_Graph")) in
  let nm = type_name name ^ "'" in
  let wrap e =
    match sh with List _ | Map _ -> [%expr fun ?flat -> [%e e]] | _ -> e
  in
  Vb.mk
    (Pat.var (Location.mknoloc (type_name name)))
    (wrap
       (Exp.fun_ Nolabel None
          [%pat? ([%p Pat.var (Location.mknoloc nm)] : [%t type_ident name])]
          (Exp.constraint_
             (match sh with
             | Blob -> [%expr Converters.To_Graph.blob [%e ident nm]]
             | Boolean -> [%expr Converters.To_Graph.boolean [%e ident nm]]
             | String -> [%expr Converters.To_Graph.string [%e ident nm]]
             | Enum l ->
                 [%expr
                   Converters.To_Graph.string
                     [%e
                       Exp.match_ (ident nm)
                         (List.map
                            (fun ((nm, _, _) as enum) ->
                              Exp.case (pat_construct nm None)
                                (Exp.constant
                                   (Const.string
                                      (member_name ~name:"smithy.api#enumValue"
                                         enum))))
                            l)]]
             | Integer -> [%expr Converters.To_Graph.integer [%e ident nm]]
             | Long -> [%expr Converters.To_Graph.long [%e ident nm]]
             | Float | Double -> [%expr Converters.To_Graph.float [%e ident nm]]
             | Timestamp -> [%expr Converters.To_Graph.timestamp [%e ident nm]]
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
                         [%e e] ~name:[%e Exp.constant (Const.string name)]] )
                 |> fun e ->
                 [%expr
                   [%e e] [%e converter ~path ~sparse id] ?flat [%e ident nm]]
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
                             [%e e] ~name:[%e Exp.constant (Const.string nm)]]
                     )
                   |> fun e ->
                     match key_name with
                     | None -> e
                     | Some nm ->
                         [%expr
                           [%e e] ~key_name:[%e Exp.constant (Const.string nm)]]
                   )
                 |> fun e ->
                   match value_name with
                   | None -> e
                   | Some nm ->
                       [%expr
                         [%e e] ~value_name:[%e Exp.constant (Const.string nm)]]
                 )
                 |> fun e ->
                 [%expr
                   [%e e] [%e converter ~path ~sparse value] ?flat [%e ident nm]]
             | Structure [] -> [%expr Converters.To_Graph.structure []]
             | Structure l ->
                 Exp.match_ (ident nm)
                   [
                     Exp.case
                       (Pat.record
                          (List.map
                             (fun (nm, _, _) ->
                               ( Location.mknoloc
                                   (Longident.Lident (field_name nm)),
                                 Pat.var
                                   (Location.mknoloc (field_name nm ^ "'")) ))
                             l)
                          Closed)
                       (structure_graph_converter ~protocol l);
                   ]
             | Union l ->
                 [%expr
                   Converters.To_Graph.structure
                     [
                       [%e
                         Exp.match_ (ident nm)
                           (List.map
                              (fun ((nm, _, _) as constr) ->
                                Exp.case
                                  (pat_construct nm (Some ([], [%pat? x])))
                                  (field_graph_converter ~protocol ~param:"x"
                                     constr))
                              l)];
                     ]]
             | Service _ | Resource _ | Operation _ -> assert false)
             [%type: Converters.To_Graph.t])))

let print_to_graph ~protocol shs =
  [%str
    module To_Graph = struct
      [%%i
      Str.value Recursive (List.map (to_graph ~protocol) (IdMap.bindings shs))]
    end]

let from_json (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "From_JSON")) in
  let nm = type_name name ^ "'" in
  Vb.mk
    (Pat.var (Location.mknoloc (type_name name)))
    (Exp.fun_ Nolabel None
       [%pat? ([%p Pat.var (Location.mknoloc nm)] : Yojson.Safe.t)]
       (Exp.constraint_
          (match sh with
          | Blob -> [%expr Converters.From_JSON.blob [%e ident nm]]
          | Boolean -> [%expr Converters.From_JSON.boolean [%e ident nm]]
          | String -> [%expr Converters.From_JSON.string [%e ident nm]]
          | Enum l ->
              Exp.match_
                [%expr Converters.From_JSON.string [%e ident nm]]
                (List.map
                   (fun ((nm, _, _) as enum) ->
                     Exp.case
                       (Pat.constant
                          (Const.string
                             (member_name ~name:"smithy.api#enumValue" enum)))
                       (Exp.construct
                          (Location.mknoloc (Longident.Lident (constr_name nm)))
                          None))
                   l
                @ [ Exp.case [%pat? _] [%expr assert false] ])
          | Integer -> [%expr Converters.From_JSON.integer [%e ident nm]]
          | Long -> [%expr Converters.From_JSON.long [%e ident nm]]
          | Float | Double -> [%expr Converters.From_JSON.float [%e ident nm]]
          | Timestamp -> [%expr Converters.From_JSON.timestamp [%e ident nm]]
          | Document -> [%expr Converters.From_JSON.document [%e ident nm]]
          | List (id, _) ->
              let sparse = List.mem_assoc "smithy.api#sparse" traits in
              [%expr
                Converters.From_JSON.list [%e converter ~path ~sparse id]
                  [%e ident nm]]
          | Map (_key, (value, _)) ->
              let sparse = List.mem_assoc "smithy.api#sparse" traits in
              [%expr
                Converters.From_JSON.map
                  [%e converter ~path ~sparse value]
                  [%e ident nm]]
          | Structure [] -> [%expr ()]
          | Structure l ->
              [%expr
                let x = Converters.From_JSON.structure [%e ident nm] in
                [%e
                  Exp.record
                    (List.map
                       (fun ((nm, typ, _) as field) ->
                         let optional = optional_member field in
                         let label =
                           Location.mknoloc (Longident.Lident (field_name nm))
                         in
                         ( label,
                           [%expr
                             [%e converter ~path ~sparse:optional typ]
                               (List.assoc
                                  [%e
                                    Exp.constant
                                      (Const.string (member_name field))]
                                  x)] )) (*ZZZ Not_found? *)
                       l)
                    None]]
          | Union l ->
              Exp.match_
                [%expr Converters.From_JSON.structure [%e ident nm]]
                (List.map
                   (fun ((nm, typ, _) as constr) ->
                     Exp.case
                       [%pat?
                         [
                           ( [%p
                               Pat.constant (Const.string (member_name constr))],
                             x );
                         ]]
                       (Exp.construct
                          (Location.mknoloc (Longident.Lident (constr_name nm)))
                          (Some
                             [%expr [%e (converter ~path ~sparse:false) typ] x])))
                   l
                @ [ Exp.case [%pat? _] [%expr assert false] ])
          | Service _ | Resource _ | Operation _ -> assert false)
          (type_ident name)))

let print_from_json shs =
  [%str
    module From_JSON = struct
      [%%i Str.value Recursive (List.map from_json (IdMap.bindings shs))]
    end]

let from_xml (name, (sh, traits)) =
  let path = Longident.(Ldot (Lident "Converters", "From_XML")) in
  let nm = type_name name ^ "'" in
  let wrap e =
    match sh with List _ | Map _ -> [%expr fun ?flat -> [%e e]] | _ -> e
  in
  Vb.mk
    (Pat.var (Location.mknoloc (type_name name)))
    (wrap
       (Exp.fun_ Nolabel None
          [%pat? ([%p Pat.var (Location.mknoloc nm)] : Converters.From_XML.t)]
          (Exp.constraint_
             (match sh with
             | Blob -> [%expr Converters.From_XML.blob [%e ident nm]]
             | Boolean -> [%expr Converters.From_XML.boolean [%e ident nm]]
             | String -> [%expr Converters.From_XML.string [%e ident nm]]
             | Enum l ->
                 Exp.match_
                   [%expr Converters.From_XML.string [%e ident nm]]
                   (List.map
                      (fun ((nm, _, _) as enum) ->
                        Exp.case
                          (Pat.constant
                             (Const.string
                                (member_name ~name:"smithy.api#enumValue" enum)))
                          (Exp.construct
                             (Location.mknoloc
                                (Longident.Lident (constr_name nm)))
                             None))
                      l
                   @ [ Exp.case [%pat? _] [%expr assert false] ])
             | Integer -> [%expr Converters.From_XML.integer [%e ident nm]]
             | Long -> [%expr Converters.From_XML.long [%e ident nm]]
             | Float | Double -> [%expr Converters.From_XML.float [%e ident nm]]
             | Timestamp -> [%expr Converters.From_XML.timestamp [%e ident nm]]
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
                         [%e e] ~name:[%e Exp.constant (Const.string name)]] )
                 |> fun e ->
                 [%expr
                   [%e e] [%e converter ~path ~sparse id] ?flat [%e ident nm]]
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
                           [%e e] ~key_name:[%e Exp.constant (Const.string nm)]]
                   )
                 |> fun e ->
                   match value_name with
                   | None -> e
                   | Some nm ->
                       [%expr
                         [%e e] ~value_name:[%e Exp.constant (Const.string nm)]]
                 )
                 |> fun e ->
                 [%expr [%e e] [%e converter ~path ~sparse value] [%e ident nm]]
             | Structure [] -> [%expr ()]
             | Structure l ->
                 let x = ident nm in
                 [%expr
                   [%e
                     Exp.record
                       (List.map
                          (fun ((nm, typ, _) as field) ->
                            ( Location.mknoloc (Longident.Lident (field_name nm)),
                              let optional = optional_member field in
                              let name =
                                Exp.constant
                                  (Const.string
                                     (member_name ~name:"smithy.api#xmlName"
                                        field))
                              in
                              let flat = flattened_member field in
                              let conv = converter ~path ~sparse:false typ in
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
                 Exp.match_
                   [%expr Converters.From_XML.union [%e ident nm]]
                   (List.map
                      (fun ((nm, typ, _) as constr) ->
                        Exp.case
                          [%pat?
                            ( [%p
                                Pat.constant (Const.string (member_name constr))],
                              x )]
                          (Exp.construct
                             (Location.mknoloc
                                (Longident.Lident (constr_name nm)))
                             (Some
                                [%expr
                                  [%e (converter ~path ~sparse:false) typ] x])))
                      l
                   @ [ Exp.case [%pat? _] [%expr assert false] ])
             | Service _ | Resource _ | Operation _ -> assert false)
             (type_ident name))))

let print_from_xml shs =
  [%str
    module From_XML = struct
      [%%i Str.value Recursive (List.map from_xml (IdMap.bindings shs))]
    end]

let compute_inputs_outputs shapes =
  let rec traverse set ~direct id =
    if IdSet.mem id set then set
    else
      let set = if direct then set else IdSet.add id set in
      match IdMap.find_opt id shapes with
      | None -> set
      | Some (typ, _) -> (
          match typ with
          | Blob | Boolean | String | Enum _ | Integer | Long | Float | Double
          | Timestamp | Document ->
              set
          | Service _ | Resource _ | Operation _ -> assert false
          | List (id, _) | Map (_, (id, _)) -> traverse set ~direct:false id
          | Structure l | Union l ->
              List.fold_left
                (fun set (_, id, _) -> traverse set ~direct:false id)
                set l)
  in
  let traverse_list set lst =
    List.fold_left (fun set x -> traverse set ~direct:false x) set lst
  in
  IdMap.fold
    (fun _ (ty, _) (inputs, outputs, operations) ->
      match ty with
      | Service { errors; _ } ->
          (inputs, traverse_list outputs errors, operations)
      | Operation { input; output; errors } ->
          ( traverse inputs ~direct:true input,
            traverse_list (traverse outputs ~direct:false output) errors,
            IdSet.add input (IdSet.add output operations) )
      | _ -> (inputs, outputs, operations))
    shapes
    (IdSet.empty, IdSet.empty, IdSet.empty)

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

let compile_operation ~service_info ~protocol ~shapes nm
    { input; output; errors } traits =
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
    Some
      {
        Docstrings.docs_pre =
          (if input.namespace = "smithy.api" then None
          else
            Some
              (Docstrings.docstring
                 ("See type {!type:" ^ type_name input
                ^ "} for a description of the parameters")
                 loc));
        docs_post =
          documentation ~shapes
            ~field_refs:
              (StringMap.empty |> field_refs output |> field_refs input)
            traits;
      }
  in
  (* Method: POST, uri: / *)
  let host_prefix =
    Option.map
      (fun e -> Util.(e |> member "hostPrefix" |> to_string))
      (List.assoc_opt "smithy.api#endpoint" traits)
  in
  let builder =
    let fields =
      if input = { namespace = "smithy.api"; identifier = "Unit" } then []
      else
        match IdMap.find input shapes with
        | Structure l, _ -> l
        | _ -> assert false
    in
    [%expr
      fun k ->
        [%e
          constructor_parameters ~shapes ~fields
            ~body:
              (List.fold_left
                 (fun expr (nm, _, _) ->
                   Exp.let_ Nonrecursive
                     [
                       Vb.mk
                         (Pat.var (Location.mknoloc (field_name nm ^ "'")))
                         (ident (field_name nm));
                     ]
                     expr)
                 [%expr
                   k
                     (let open! To_JSON in
                     [%e structure_json_converter fields])]
                 fields)]]
  in
  let errors = service_info.errors @ errors in
  let errors =
    List.fold_right
      (fun name lst ->
        [%expr
          ( [%e Exp.constant (Const.string name.identifier)],
            fun x ->
              [%e
                Exp.variant
                  (constr_name name.identifier)
                  (Some
                     [%expr
                       [%e
                         Exp.ident
                           (Location.mknoloc
                              (Longident.Ldot
                                 (Lident "From_JSON", type_name name)))]
                         x])] )
          :: [%e lst]])
      errors [%expr []]
  in
  assert (not (String.contains (Option.value ~default:"" host_prefix) '{'));
  Str.value Nonrecursive
    [
      Vb.mk ?docs
        (Pat.var (Location.mknoloc (type_name nm)))
        [%expr
          fun () ->
            Converters.create_JSON_operation
              ~variant:
                [%e
                  match protocol with
                  | `AwsJson1_0 -> Exp.variant "AwsJson1_0" None
                  | `AwsJson1_1 -> Exp.variant "AwsJson1_1" None]
              ~host_prefix:
                [%e
                  match host_prefix with
                  | None -> [%expr None]
                  | Some host_prefix ->
                      [%expr Some [%e Exp.constant (Const.string host_prefix)]]]
              ~target:[%e Exp.constant (Const.string nm.identifier)]
              ~builder:[%e builder]
              ~parser:
                [%e
                  Exp.ident
                    (Location.mknoloc
                       (if output.namespace = "smithy.api" then
                        Longident.(
                          Ldot
                            ( Ldot (Lident "Converters", "From_JSON"),
                              type_name output ))
                       else Longident.Ldot (Lident "From_JSON", type_name output)))]
              ~errors:[%e errors]];
    ]

let compile_operations ~service_info ~protocol ~shapes =
  let compile shs =
    List.rev
    @@ IdMap.fold
         (fun nm (ty, traits) rem ->
           match ty with
           | Operation info ->
               compile_operation ~service_info ~protocol ~shapes nm info traits
               :: rem
           | _ -> rem)
         shs []
  in
  let shs, ops =
    IdMap.fold
      (fun nm (typ, traits) (shs, ops) ->
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
            let add x l = match x with None -> l | Some x -> x :: l in
            let operations =
              r.operations @ r.collection_operations
              |> add r.create |> add r.put |> add r.read |> add r.update
              |> add r.delete |> add r.list
            in
            ( List.fold_left (fun shs x -> IdMap.remove x shs) shs operations,
              (Str.text [ Docstrings.docstring ("{2 " ^ name ^ "}") loc ]
              @ (match documentation ~shapes traits with
                | None -> []
                | Some doc -> Str.text [ doc ])
              @ compile
                  (List.fold_left
                     (fun shs' id -> IdMap.add id (IdMap.find id shs) shs')
                     IdMap.empty operations))
              :: ops )
        | _ -> (shs, ops))
      shapes (shapes, [])
  in
  compile shs @ List.concat (List.rev ops)

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

let to_caml_list l =
  List.fold_right (fun e rem -> [%expr [%e e] :: [%e rem]]) l [%expr []]

let tag_re =
  Re.(
    compile
      (seq
         [
           char '{';
           group (rep1 (diff any (set "#}")));
           opt (seq [ char '#'; group (rep1 (diff any (set "}"))) ]);
           char '}';
         ]))

let compile_string s =
  match Re.split_full tag_re s with
  | [ `Text s ] -> Exp.constant (Const.string s)
  | l ->
      let l =
        List.map
          (fun x ->
            match x with
            | `Text s -> Exp.constant (Const.string s)
            | `Delim g -> (
                let t = uncapitalized_identifier (Re.Group.get g 1) in
                let t =
                  if t = "endpoint" then [%expr Uri.to_string [%e ident t]]
                  else ident t
                in
                match Re.Group.get_opt g 2 with
                | None -> t
                | Some f ->
                    Exp.field t
                      (Location.mknoloc
                         Longident.(
                           Ldot
                             ( Ldot (Lident "Converters", "Endpoint"),
                               uncapitalized_identifier f )))))
          l
      in

      [%expr String.concat "" [%e to_caml_list l]]

let array_access_re =
  Re.(
    compile
      (whole_string
         (seq [ group (rep any); char '['; group (rep any); char ']' ])))

let rec compile_expr e =
  match e with
  | Bool b -> if b then [%expr true] else [%expr false]
  | String s -> compile_string s
  | Int i -> Exp.constant (Const.int i)
  | Var nm -> ident (uncapitalized_identifier nm)
  | Apply { f; args; _ } -> (
      let fn f =
        Exp.ident
          (Location.mknoloc
             Longident.(Ldot (Ldot (Lident "Converters", "Endpoint"), f)))
      in
      match (f, args) with
      | "aws.isVirtualHostableS3Bucket", [ b; b' ] ->
          [%expr
            [%e fn "is_virtual_hostable_s3_bucket"]
              [%e compile_expr b] [%e compile_expr b']]
      | "aws.parseArn", [ b ] -> [%expr [%e fn "parse_arn"] [%e compile_expr b]]
      | "aws.partition", [ r ] ->
          [%expr [%e fn "partition"] [%e compile_expr r]]
      | "booleanEquals", ([ e; Bool true ] | [ Bool true; e ]) -> compile_expr e
      | "booleanEquals", [ e; Bool false ] -> [%expr not [%e compile_expr e]]
      | "booleanEquals", [ e; e' ] ->
          [%expr [%e compile_expr e] = [%e compile_expr e']]
      | "getAttr", [ r; String s ] -> (
          match Re.exec_opt array_access_re s with
          | Some g ->
              [%expr
                [%e fn "array_get_opt"]
                  [%e
                    Exp.field (compile_expr r)
                      (Location.mknoloc
                         Longident.(
                           Ldot
                             ( Ldot (Lident "Converters", "Endpoint"),
                               uncapitalized_identifier (Re.Group.get g 1) )))]
                  [%e
                    Exp.constant (Const.int (int_of_string (Re.Group.get g 2)))]]
          | None ->
              Exp.field (compile_expr r)
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
          [%expr Some (Uri.pct_encode ~component:`Generic [%e compile_expr e])]
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
                            [%e Exp.constant (Const.string k)],
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
            let* [%p Pat.var (Location.mknoloc (uncapitalized_identifier x))] =
              [%e e]
            in
            [%e rem]]
      | Apply { f = "isSet"; args = [ Var x ]; _ } ->
          let x = uncapitalized_identifier x in
          [%expr
            let* [%p Pat.var (Location.mknoloc x)] = [%e ident x] in
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
      signing_region = Util.(s |> member "signingRegion" |> to_option to_string);
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
      Exp.fun_
        (if (*p = "region" ||*) required && default = None then Labelled p
        else Optional p)
        (Option.map
           (fun b -> if b then [%expr true] else [%expr false])
           default)
        (Pat.var (Location.mknoloc p))
        rem)
    parameters
    [%expr
      fun () ->
        let open Converters.Endpoint.Rules in
        [%e compile_rule_list rules]]

let protocols =
  [
    ("aws.protocols#awsJson1_0", `AwsJson1_1);
    ("aws.protocols#awsJson1_1", `AwsJson1_0);
    ("aws.protocols#restJson1", `RestJson1);
    ("aws.protocols#restXml", `RestXml);
    ("aws.protocols#awsQuery", `AwsQuery);
    ("aws.protocols#ec2Query", `Ec2Query);
  ]

let compile dir f =
  let shs = parse (Filename.concat dir f) in
  let service =
    IdMap.choose
      (IdMap.filter
         (fun _ (typ, _) -> match typ with Service _ -> true | _ -> false)
         shs)
  in
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
  let ch = open_out (Filename.concat "generated" (name ^ ".ml")) in
  let f = Format.formatter_of_out_channel ch in
  let inputs, outputs, operations = compute_inputs_outputs shs in
  let types = print_types ~inputs ~operations shs in
  let input_shapes = IdMap.filter (fun id _ -> IdSet.mem id inputs) shs in
  let record_constructors = print_constructors input_shapes in
  let converters =
    match protocol with
    | `AwsJson1_0 | `AwsJson1_1 | `RestJson1 -> print_to_json input_shapes
    | `RestXml -> print_to_xml input_shapes
    | (`AwsQuery | `Ec2Query) as protocol ->
        print_to_graph ~protocol input_shapes
  in
  let output_shapes = IdMap.filter (fun id _ -> IdSet.mem id outputs) shs in
  let converters' =
    match protocol with
    | `AwsJson1_0 | `AwsJson1_1 | `RestJson1 -> print_from_json output_shapes
    | `RestXml | `AwsQuery | `Ec2Query -> print_from_xml output_shapes
  in
  let toplevel_doc =
    let _, (_, traits) = service in
    match documentation ~shapes:shs traits with
    | None -> []
    | Some doc -> Str.text [ doc ]
  in
  let service_info =
    match service with _, (Service info, _) -> info | _ -> assert false
  in
  let endpoint =
    [
      Str.value Nonrecursive
        [
          Vb.mk
            (Pat.var (Location.mknoloc "aws_endpoint"))
            (handle_endpoint
               (List.assoc "smithy.rules#endpointRuleSet" (snd (snd service))));
        ];
    ]
  in
  let operations =
    match protocol with
    | (`AwsJson1_1 | `AwsJson1_0) as protocol ->
        compile_operations ~service_info ~protocol ~shapes:shs
    | _ -> []
  in
  let toggle_hide = Str.text [ Docstrings.docstring "/*" loc ] in
  Format.fprintf f "%a@." Pprintast.structure
    (toplevel_doc
    @ Str.text [ Docstrings.docstring "{1 Type definitions}" loc ]
    @ types
      ::
      (if record_constructors = [] then []
      else Str.text [ Docstrings.docstring "{1 Record constructors}" loc ])
    @ record_constructors @ toggle_hide @ endpoint @ converters @ converters'
    @ toggle_hide
    @ Str.text [ Docstrings.docstring "{1 Operations}" loc ]
    @ operations);
  close_out ch

let () =
  let _f { namespace = _; identifier = _ } = () in
  let dir = "/home/jerome/sources/aws-sdk-rust/aws-models" in
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
