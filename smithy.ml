(*
- mli files

- serializer / deserializer      Json / XML

- timestampFormat

- validation: length, pattern, range, uniqueItems

- documentation

      4 aws.protocols#restXml     <== S3
     17 aws.protocols#awsQuery
     23 aws.protocols#awsJson1_0
    106 aws.protocols#awsJson1_1
    186 aws.protocols#restJson1

./gradlew :smithy-aws-protocol-tests:build

- doc:
  - also search references in field of input / output ==> {!type-t.field}
  - document key of each StringMap.t  ('a, 'b) map = 'b StringMap.t ???

Things to consider
- endpoint configuration
- streaming
- pagination ==> modification of the request / access to the response
- retries ==> retryable errors / idempotency
- presigned URLs

Compiling an operation:
- builder function (straight for arguments to JSon)
  ==> json + host prefix + uri ?
*)

open Yojson.Safe

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
  | Document (* ??? *)
  | List of shape_id
  | Map of shape_id * shape_id
  | Structure of (string * shape_id * traits) list
  | Union of (string * shape_id * traits) list
  | Service of service
  | Resource
  | Operation of operation

and service = {
  version : string;
  operations : shape_id list;
  resources : shape_id list;
  errors : shape_id list;
}

and operation = { input : shape_id; output : shape_id; errors : shape_id list }

let parse_shape (id, sh) =
  let typ = Util.(sh |> member "type" |> to_string) in
  let parse_traits m =
    Util.(
      m |> member "traits" |> to_option to_assoc |> Option.value ~default:[])
  in
  let parse_member name =
    Util.(sh |> member name |> member "target" |> to_string |> parse_shape_id)
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
      | "resource" -> Resource
      | "operation" ->
          Operation
            {
              input = parse_member "input";
              output = parse_member "output";
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
    "external";
    "function";
    "include";
    "match";
    "method";
    "mutable";
    "object";
    "or";
    "then";
    "to";
    "type";
    "bool";
    "float";
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
      | "Integer" -> "Int32.t"
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

let optional_member (_, _, traits') =
  not
    (List.mem_assoc "smithy.api#required" traits'
    || List.mem_assoc "smithy.api#default" traits')

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

(*
let documentation =
  let escaped_re = Re.(compile (set "{}[]@")) in
  (*  let space_re = Re.(compile (rep1 (seq [ char '\\'; set "nt" ]))) in*)
  let space_re = Re.(rep1 (set " \n\t")) in
  let tag_re =
    Re.(
      compile
        (seq
           [
             group
               (seq [ char '<'; opt (char '/'); rep1 (rg 'a' 'z'); char '>' ]);
             opt space_re;
           ]))
  in
  fun traits ->
    match List.assoc_opt "smithy.api#documentation" traits with
    | Some doc ->
        let doc =
          doc |> Yojson.Safe.Util.to_string
          |> Re.replace escaped_re ~f:(fun g -> "\\" ^ Re.Group.get g 0)
          (*          |> Re.replace space_re ~f:(fun _ -> " ")*)
          |> Re.replace tag_re ~f:(fun g ->
                 match Re.Group.get g 1 with
                 | "<p>" -> if Re.Group.start g 0 = 0 then "" else "\n\n"
                 | "</p>" -> ""
                 | "<code>" -> "["
                 | "</code>" -> "]"
                 | "<ul>" -> ""
                 | "</ul>" -> "\n\n"
                 | "<li>" -> "\n- "
                 | "</li>" -> ""
                 | "<i>" -> "{i "
                 | "<b>" -> "{b "
                 | "</i>" | "</b>" -> "}"
                 | tag ->
                     Format.eprintf "ZZZZ %s@." tag;
                     "")
        in
        Some (Docstrings.docstring doc loc)
    | None -> None
*)
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

let rec format ~shapes ~in_anchor ?(in_list = false) ~toplevel doc =
  match doc with
  | Text txt -> escape_text txt
  | Element ("p", _, children) ->
      let s = format_children ~shapes ~in_anchor ~toplevel:false children in
      if toplevel then s ^ "\n\n" else s
  | Element ("code", _, children) | Element ("a", [], children) ->
      let s = escape_code (children_text children) in
      let reference =
        IdMap.filter (fun { identifier; _ } _ -> identifier = s) shapes
      in
      if not (in_anchor || IdMap.is_empty reference) then
        let _, (typ, _) = IdMap.choose reference in
        match typ with
        | Service _ | Resource -> "[" ^ s ^ "]"
        | Operation _ ->
            "[" ^ s ^ "]" (*ZZZZ "{!val:" ^ uncapitalized_identifier s ^ "}" *)
        | _ -> "{!type:" ^ uncapitalized_identifier s ^ "}"
      else "[" ^ s ^ "]"
  | Element (("i" | "replaceable" | "title"), _, children) ->
      let s = format_children ~shapes ~in_anchor ~toplevel:false children in
      if empty_text s then s else "{i " ^ s ^ "}"
  | Element ("b", _, children) ->
      let s = format_children ~shapes ~in_anchor ~toplevel:false children in
      if empty_text s then s else "{b " ^ s ^ "}"
  | Element (("note" | "para"), _, children) ->
      format_children ~shapes ~in_anchor ~toplevel children
  | Element ("important", _, children) ->
      format_children ~shapes ~in_anchor ~toplevel children
  | Element ("a", attr, children) when List.mem_assoc "href" attr ->
      let url = List.assoc "href" attr in
      let s = format_children ~shapes ~in_anchor:true ~toplevel children in
      if empty_text url then s else "{{: " ^ url ^ " }" ^ s ^ "}"
  | Element ("ul", _, children) ->
      "\n{ul "
      ^ format_children ~shapes ~in_anchor ~in_list:true ~toplevel:false
          (fix_list children)
      ^ "}\n"
  | Element ("li", _, children) ->
      let s = format_children ~shapes ~in_anchor ~toplevel:false children in
      if in_list then "{- " ^ s ^ "}" else s
  | Element ("dl", _, children) ->
      "\n{ul "
      ^ format_dl
          ~format:(format_children ~shapes ~in_anchor ~toplevel:false)
          children [] []
      ^ "}\n"
  (*
  | Element ("dt", _, children) ->
      let s = format_children ~shapes ~in_anchor ~toplevel:false children in
      if empty_text s then "{- " else "{- {b " ^ s ^ "} "
  | Element ("dd", _, children) ->
      format_children ~shapes ~in_anchor ~toplevel:false children ^ "}"
*)
  | Element ("ol", _, children) ->
      "\n{ol "
      ^ format_children ~shapes ~in_anchor ~in_list:true ~toplevel:false
          (fix_list children)
      ^ "}\n"
  | Element ("br", _, []) -> "\n\n"
  | Element ("fullname", _, _) -> ""
  | Element (nm, _, children) ->
      let s =
        "<" ^ nm ^ ">"
        ^ format_children ~shapes ~in_anchor ~toplevel:false children
      in
      (*      Format.eprintf "AAA %s@." s;*)
      s

and format_children ~shapes ~in_anchor ?(in_list = false) ~toplevel lst =
  String.concat "" (List.map (format ~shapes ~in_anchor ~in_list ~toplevel) lst)

let documentation ~shapes doc =
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
        Some (format_children ~shapes ~in_anchor:false ~toplevel:true children)
    | _ -> assert false

let documentation ~shapes ?extra traits =
  match List.assoc_opt "smithy.api#documentation" traits with
  | None -> None
  | Some doc -> (
      let doc = Yojson.Safe.Util.to_string doc in
      match documentation ~shapes doc with
      | Some doc ->
          let doc =
            match extra with None -> doc | Some extra -> doc ^ "\n\n" ^ extra
          in
          Some (Docstrings.docstring doc loc)
      | None -> (
          match extra with
          | None -> None
          | Some extra -> Some (Docstrings.docstring extra loc)))

let default_value shapes typ default =
  match default with
  | `String s -> Exp.constant (Const.string s)
  | `Int n ->
      Exp.constant
        (match type_of_shape shapes typ with
        | Float | Double -> Const.float (Printf.sprintf "%d." n)
        | Integer -> Const.int32 (Int32.of_int n)
        | Long -> Const.int64 (Int64.of_int n)
        | _ -> assert false)
  | `Bool b -> if b then [%expr true] else [%expr false]
  | _ -> assert false

let type_ident id =
  Typ.constr (Location.mknoloc (Longident.Lident (type_name id))) []

let print_constructor shapes name fields =
  let has_optionals =
    List.exists
      (fun (_, _, traits) ->
        List.mem_assoc "smithy.api#default" traits
        || not (List.mem_assoc "smithy.api#required" traits))
      fields
  in
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
  let expr =
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
      (if has_optionals then Exp.fun_ Nolabel None [%pat? ()] body else body)
  in
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
    let extra =
      match sh with
      | Structure l when l <> [] && IdSet.mem nm inputs ->
          Some
            ("See associated record builder function {!val:" ^ type_name nm
           ^ "}.")
      | _ -> None
    in
    Option.map
      (fun d -> { Docstrings.docs_pre = Some d; docs_post = None })
      (documentation ~shapes ?extra traits)
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
  | Integer -> manifest_type [%type: Int32.t]
  | Long -> manifest_type [%type: Int64.t]
  | Float | Double -> manifest_type [%type: float]
  | Timestamp -> manifest_type [%type: CalendarLib.Calendar.t]
  | Document -> manifest_type [%type: Yojson.Safe.t]
  | List id ->
      let sparse = List.mem_assoc "smithy.api#sparse" traits in
      let id = type_ident id in
      manifest_type
        [%type: [%t if sparse then [%type: [%t id] option] else id] list]
  | Map (_key, value) ->
      let sparse = List.mem_assoc "smithy.api#sparse" traits in
      let id = type_ident value in
      manifest_type
        [%type: [%t if sparse then [%type: [%t id] option] else id] StringMap.t]
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
  | Service _ | Resource | Operation _ -> assert false

let print_types ~inputs ~operations shapes =
  let types =
    IdMap.bindings
      (IdMap.filter
         (fun _ (typ, _) ->
           match typ with
           | Service _ | Operation _ | Resource -> false
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
          | List id ->
              let sparse = List.mem_assoc "smithy.api#sparse" traits in
              [%expr
                Converters.To_JSON.list [%e converter ~path ~sparse id]
                  [%e ident nm]]
          | Map (_key, value) ->
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
                    [%expr
                      Converters.To_JSON.structure
                        [%e
                          List.fold_right
                            (fun ((nm, typ, _) as field) lst ->
                              let optional = optional_member field in
                              [%expr
                                ( [%e
                                    Exp.constant
                                      (Const.string (member_name field))],
                                  [%e (converter ~path ~sparse:optional) typ]
                                    [%e ident (field_name nm ^ "'")] )
                                :: [%e lst]])
                            l [%expr []]]];
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
          | Service _ | Resource | Operation _ -> assert false)
          [%type: Yojson.Safe.t]))

let print_to_json shs =
  [%str
    module To_JSON = struct
      [%%i Str.value Recursive (List.map to_json (IdMap.bindings shs))]
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
          | List id ->
              let sparse = List.mem_assoc "smithy.api#sparse" traits in
              [%expr
                Converters.From_JSON.list [%e converter ~path ~sparse id]
                  [%e ident nm]]
          | Map (_key, value) ->
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
                                  x)] ))
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
          (*
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
*)
          | Service _ | Resource | Operation _ -> assert false)
          (type_ident name)))

let print_from_json shs =
  [%str
    module From_JSON = struct
      [%%i Str.value Recursive (List.map from_json (IdMap.bindings shs))]
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
          | Service _ | Resource | Operation _ -> assert false
          | List id | Map (_, id) -> traverse set ~direct:false id
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
~parser:From_JSON.result
~builder:(fun k ~a ?(b =...) ~c -> k (To_Json.structure ...))
~errors:[("name", (retryable, fun x -> `Foo (From_Json.foo) x))); ...]
*)
let compile_operation ~service_info ~shapes nm { input; output; errors } traits
    =
  let docs =
    Option.map
      (fun d -> { Docstrings.docs_pre = Some d; docs_post = None })
      (documentation ~shapes traits)
  in
  (* Method: POST, uri: / *)
  let host_prefix =
    Option.map
      (fun e -> Util.(e |> member "hostPrefix" |> to_string))
      (List.assoc_opt "smithy.api#endpoint" traits)
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
  ignore (shapes, nm, input, output, errors, traits);
  Str.value Nonrecursive
    [
      Vb.mk ?docs
        (Pat.var (Location.mknoloc (type_name nm)))
        [%expr
          fun () ->
            Converters.create_JSON_operation ~variant:`AwsJson1_0
              ~host_prefix:
                [%e
                  match host_prefix with
                  | None -> [%expr None]
                  | Some host_prefix ->
                      [%expr Some [%e Exp.constant (Const.string host_prefix)]]]
              ~target:[%e Exp.constant (Const.string nm.identifier)]
              ~builder:(fun k () -> k `Null)
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

let compile_operations ~service_info ~shapes =
  List.rev
  @@ IdMap.fold
       (fun nm (ty, traits) rem ->
         match ty with
         | Operation info ->
             compile_operation ~service_info ~shapes nm info traits :: rem
         | _ -> rem)
       shapes []

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
  let ch = open_out (Filename.concat "generated" (name ^ ".ml")) in
  let f = Format.formatter_of_out_channel ch in
  let inputs, outputs, operations = compute_inputs_outputs shs in
  let types = print_types ~inputs ~operations shs in
  let input_shapes = IdMap.filter (fun id _ -> IdSet.mem id inputs) shs in
  let record_constructors = print_constructors input_shapes in
  let converters = print_to_json input_shapes in
  let output_shapes = IdMap.filter (fun id _ -> IdSet.mem id outputs) shs in
  let converters' = print_from_json output_shapes in
  let toplevel_doc =
    let _, (_, traits) = service in
    match documentation ~shapes:shs traits with
    | None -> []
    | Some doc -> Str.text [ doc ]
  in
  let service_info =
    match service with _, (Service info, _) -> info | _ -> assert false
  in
  let operations =
    if
      let _, (_, traits) = service in
      List.mem_assoc "aws.protocols#awsJson1_1" traits
      || List.mem_assoc "aws.protocols#awsJson1_0" traits
    then compile_operations ~service_info ~shapes:shs
    else []
  in
  Format.fprintf f "%a@." Pprintast.structure
    (toplevel_doc
    @ Str.module_
        (Mb.mk
           (Location.mknoloc (Some "StringMap"))
           (Mod.ident
              (Location.mknoloc
                 Longident.(Ldot (Lident "Converters", "StringMap")))))
      :: Str.text [ Docstrings.docstring "{1 Type definitions}" loc ]
    @ (types :: Str.text [ Docstrings.docstring "{1 Record constructors}" loc ])
    @ record_constructors @ converters @ converters'
    @ Str.text [ Docstrings.docstring "{1 Operations}" loc ]
    @ operations);
  close_out ch

let () =
  let _f { namespace = _; identifier = _ } = () in
  let dir = "/home/jerome/aws-sdk-js-v3/codegen/sdk-codegen/aws-models" in
  if true then
    let files = Sys.readdir dir in
    Array.iter (fun f -> compile dir f) files
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
