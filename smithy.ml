(*
- mli files

- serializer / deserializer      Json / XML
  ==> mark inputs and ouputs

- float: +/-infinity

- deal especially with operation requests (does not define a type) and
  errors (define a union)

- constructors only for operation inputs

- validation: length, pattern, range, uniqueItems

- documentation

      4 aws.protocols#restXml
     17 aws.protocols#awsQuery
     23 aws.protocols#awsJson1_0
    106 aws.protocols#awsJson1_1
    186 aws.protocols#restJson1
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
  | Service
  | Resource
  | Operation of { input : shape_id; output : shape_id }

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
      | "service" -> Service
      | "resource" -> Resource
      | "operation" ->
          Operation
            { input = parse_member "input"; output = parse_member "output" }
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
      | _ -> assert false)
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

let type_constructor ?arg_type nm =
  Type.constructor
    ?args:
      (Option.map
         (fun typ -> Parsetree.Pcstr_tuple [ type_ident typ ])
         arg_type)
    (Location.mknoloc (constr_name nm))

let print_type (nm, (sh, traits)) =
  let manifest_type manifest =
    Type.mk ~manifest (Location.mknoloc (type_name nm))
  in
  match sh with
  | Blob -> manifest_type [%type: string]
  | Boolean -> manifest_type [%type: bool]
  | String -> manifest_type [%type: string]
  | Enum l ->
      let l = List.map (fun (nm, _, _) -> type_constructor nm) l in
      Type.mk ~kind:(Ptype_variant l) (Location.mknoloc (type_name nm))
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
          (fun ((nm, typ, _) as field) ->
            let optional = optional_member field in
            let id = type_ident typ in
            Type.field
              (Location.mknoloc (field_name nm))
              (if optional then [%type: [%t id] option] else id))
          l
      in
      Type.mk ~kind:(Ptype_record l) (Location.mknoloc (type_name nm))
  | Union l ->
      let l =
        List.map
          (fun (nm, typ, _) ->
            assert (typ <> { namespace = "smithy.api"; identifier = "Unit" });
            type_constructor ~arg_type:typ nm)
          l
      in
      Type.mk ~kind:(Ptype_variant l) (Location.mknoloc (type_name nm))
  | Service | Resource | Operation _ -> assert false

let print_types shs =
  Str.type_ Recursive
    (List.map print_type
       (IdMap.bindings
          (IdMap.filter
             (fun _ (typ, _) ->
               match typ with
               | Service | Operation _ | Resource -> false
               | _ -> true)
             shs)))

let ident id = Exp.ident (Location.mknoloc (Longident.Lident id))

let converter ~sparse id =
  let convert id =
    match id.namespace with
    | "smithy.api" -> (
        match id.identifier with
        | "Boolean" | "PrimitiveBoolean" -> [%expr Converters.To_JSON.boolean]
        | "Blob" -> [%expr Converters.To_JSON.blob]
        | "String" -> [%expr Converters.To_JSON.string]
        | "Integer" -> [%expr Converters.To_JSON.integer]
        | "Long" | "PrimitiveLong" -> [%expr Converters.To_JSON.long]
        | "Float" | "Double" -> [%expr Converters.To_JSON.float]
        | "Timestamp" -> [%expr Converters.To_JSON.timestamp]
        | "Document" -> [%expr Converters.To_JSON.document]
        | _ -> assert false)
    | _ -> ident (type_name id)
  in
  if sparse then [%expr Converters.To_JSON.option [%e convert id]]
  else convert id

let member_name ?(name = "smithy.api#jsonName") (nm, _, traits) =
  try Yojson.Safe.Util.to_string (List.assoc name traits) with Not_found -> nm

let pat_construct nm =
  Pat.construct (Location.mknoloc (Longident.Lident (constr_name nm)))

let to_json (name, (sh, traits)) =
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
                Converters.To_JSON.list [%e converter ~sparse id] [%e ident nm]]
          | Map (_key, value) ->
              let sparse = List.mem_assoc "smithy.api#sparse" traits in
              [%expr
                Converters.To_JSON.map [%e converter ~sparse value]
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
                                  [%e (converter ~sparse:optional) typ]
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
                                   [%e (converter ~sparse:false) typ] x])
                           l)];
                  ]]
          | Service | Resource | Operation _ -> assert false)
          [%type: Yojson.Safe.t]))

let print_to_json shs =
  [%str
    module To_JSON = struct
      [%%i Str.value Recursive (List.map to_json (IdMap.bindings shs))]
    end]

let compute_inputs shapes =
  let rec traverse inputs ~direct id =
    if IdSet.mem id inputs then inputs
    else
      let inputs = if direct then inputs else IdSet.add id inputs in
      match IdMap.find_opt id shapes with
      | None -> inputs
      | Some (typ, _) -> (
          match typ with
          | Blob | Boolean | String | Enum _ | Integer | Long | Float | Double
          | Timestamp | Document ->
              inputs
          | Service | Resource | Operation _ -> assert false
          | List id | Map (_, id) -> traverse inputs ~direct:false id
          | Structure l | Union l ->
              List.fold_left
                (fun inputs (_, id, _) -> traverse inputs ~direct:false id)
                inputs l)
  in
  IdMap.fold
    (fun _ (ty, _) inputs ->
      match ty with
      | Operation { input; _ } -> traverse inputs ~direct:true input
      | _ -> inputs)
    shapes IdSet.empty

let compile dir f =
  let shs = parse (Filename.concat dir f) in
  let name =
    let sdk_id =
      let _, (_, traits) =
        IdMap.choose
          (IdMap.filter
             (fun _ (typ, _) -> match typ with Service -> true | _ -> false)
             shs)
      in
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
  Format.fprintf f "module StringMap = Converters.StringMap@.";
  let types = print_types shs in
  let inputs = compute_inputs shs in
  let input_shapes = IdMap.filter (fun id _ -> IdSet.mem id inputs) shs in
  let record_constructors = print_constructors input_shapes in
  let converters = print_to_json input_shapes in
  Format.fprintf f "%a@." Pprintast.structure
    ((types :: record_constructors) @ converters);
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

type query = string
type response = string

type ('a, 'b, 'c) operation =
  { build :  (query -> 'b) -> 'a;
    parse : response -> 'c }

let perform ~f op =
   op.build (fun query -> Lwt.bind (f query) (fun resp -> Lwt.return (op.parse resp)))
;;

let build k ~x ~y = k (x ^ y);;
let parse x = x, x

let op = {build; parse}

fun f -> perform ~f op;;
*)
