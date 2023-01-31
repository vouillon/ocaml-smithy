(*
- mli files

- converter enum to string (more generally, serializer / deserializer)
  ==> mark inputs and ouputs

let rec toto toto' : Yojson.Safe.t =
  let {kls = kls'; jffs} : foo = toto' in
  `Assoc ["kls", kls kls'; ...]
and ...
`String (Base64.encode_string s)


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
  | Operation

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
      | "operation" -> Operation
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
  let replace re s =
    Re.replace re ~f:(fun g -> Re.Group.get g 1 ^ "_" ^ Re.Group.get g 2) s
  in
  fun s ->
    s |> replace first_pattern_re |> replace second_pattern_re
    |> String.lowercase_ascii

let reserved_words =
  [
    "option";
    "end";
    "and";
    "type";
    "object";
    "string";
    "then";
    "else";
    "or";
    "method";
    "constraint";
    "float";
    "match";
    "function";
    "bool";
    "to";
    "mutable";
    "include";
    "begin";
    "external";
    "None";
  ]

let uncapitalized_identifier s =
  let s = to_snake_case s in
  if List.mem s reserved_words then s ^ "_" else s

let all_upper_re =
  Re.(compile (whole_string (rep (alt [ rg 'A' 'Z'; rg '0' '9'; char '_' ]))))

let capitalized_identifier s =
  let s =
    if Re.execp all_upper_re s then s
    else String.capitalize_ascii (to_snake_case s)
  in
  if List.mem s reserved_words then s ^ "_" else s

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

let print_constructor f shapes name fields =
  Format.fprintf f "@[<2>let @[<2>%s" (type_name name);
  let has_optionals =
    List.fold_left
      (fun opt field ->
        let nm, typ, traits' = field in
        let optional = optional_member field in
        match List.assoc_opt "smithy.api#default" traits' with
        | Some default when default <> `Null ->
            Format.fprintf f "@ ?(%s = %a)" (field_name nm)
              (fun f default ->
                match default with
                | `String s -> Format.fprintf f "\"%s\"" s
                | `Int n -> (
                    match type_of_shape shapes typ with
                    | Float | Double -> Format.fprintf f "%d." n
                    | Integer -> Format.fprintf f "%dl" n
                    | Long -> Format.fprintf f "%dL" n
                    | _ -> assert false)
                | `Bool b ->
                    Format.fprintf f "%s" (if b then "true" else "false")
                | _ -> Format.eprintf "%s@." (Yojson.Safe.to_string default))
              default;
            true
        | _ ->
            Format.fprintf f "@ %s%s"
              (if optional then "?" else "~")
              (field_name nm);
            opt || optional)
      false fields
  in
  if has_optionals then Format.fprintf f "@ ()";
  Format.fprintf f "@]@ : %s =@ @[<hv>{" (type_name name);
  Format.pp_print_list
    ~pp_sep:(fun f () -> Format.fprintf f "@ ;")
    (fun f (nm, _, _) -> Format.fprintf f " %s" (field_name nm))
    f fields;
  Format.fprintf f " }@]@]@."

let print_constructors f shapes =
  IdMap.iter
    (fun name (sh, _) ->
      match sh with
      | Structure l when l <> [] -> print_constructor f shapes name l
      | _ -> ())
    shapes

let print_type f (nm, (sh, traits)) =
  Format.fprintf f " %s =@ " (type_name nm);
  (match sh with
  | Blob -> Format.fprintf f "string"
  | Boolean -> Format.fprintf f "bool"
  | String -> Format.fprintf f "string"
  | Enum l ->
      Format.pp_print_list
        ~pp_sep:(fun f () -> Format.fprintf f "@ ")
        (fun f (nm, _, _) -> Format.fprintf f "| %s" (constr_name nm))
        f l
  | Integer -> Format.fprintf f "Int32.t"
  | Long -> Format.fprintf f "Int64.t"
  | Float -> Format.fprintf f "float"
  | Double -> Format.fprintf f "float"
  | Timestamp -> Format.fprintf f "CalendarLib.Calendar.t"
  | Document -> Format.fprintf f "Yojson.Safe.t"
  | List id ->
      let sparse = List.mem_assoc "smithy.api#sparse" traits in
      Format.fprintf f "%s %slist" (type_name id)
        (if sparse then "option " else "")
  | Map (key, value) ->
      let sparse = List.mem_assoc "smithy.api#sparse" traits in
      Format.fprintf f "(*%s ->*) %s %sStringMap.t" (type_name key)
        (type_name value)
        (if sparse then "option " else "")
  | Structure [] -> Format.fprintf f "unit"
  | Structure l ->
      Format.fprintf f "@[<hv>{";
      Format.pp_print_list
        ~pp_sep:(fun f () -> Format.fprintf f "@ ;")
        (fun f ((nm, typ, _) as field) ->
          let optional = optional_member field in
          Format.fprintf f " %s : %s%s" (field_name nm) (type_name typ)
            (if optional then " option" else ""))
        f l;
      Format.fprintf f " }@]"
  | Union l ->
      Format.pp_print_list
        ~pp_sep:(fun f () -> Format.fprintf f "@ ")
        (fun f (nm, typ, _) ->
          assert (typ <> { namespace = "smithy.api"; identifier = "Unit" });
          Format.fprintf f "| %s of %s" (constr_name nm) (type_name typ))
        f l
  | Service | Resource | Operation -> assert false);
  Format.fprintf f "@]@."

let print_types f shs =
  Format.fprintf f "@[<hv2>type";
  Format.pp_print_list
    ~pp_sep:(fun f () -> Format.fprintf f "@[<hv2>and")
    print_type f
    (IdMap.bindings
       (IdMap.filter
          (fun _ (typ, _) ->
            match typ with Service | Operation | Resource -> false | _ -> true)
          shs))

let converter ~sparse f id =
  let convert =
    match id.namespace with
    | "smithy.api" -> (
        match id.identifier with
        | "Boolean" | "PrimitiveBoolean" -> "(fun x -> `Bool x)"
        | "Blob" | "String" -> "(fun x -> `String x)"
        | "Integer" -> "(fun x -> `Intlit (Int32.to_string x))"
        | "Long" | "PrimitiveLong" -> "(fun x -> `Intlit (Int64.to_string x))"
        | "Float" | "Double" -> "(fun x -> `Float x)"
        | "Timestamp" ->
            "(fun x -> `Float (CalendarLib.Calendar.to_unixfloat x))"
        | "Document" -> "(fun x -> x)"
        | _ -> assert false)
    | _ -> type_name id
  in
  Format.fprintf f "%s"
    (if sparse then
     "(fun x -> match x with | None -> `Null | Some x -> " ^ convert ^ " x)"
    else convert)

let to_json f (name, (sh, traits)) =
  let nm = type_name name ^ "'" in
  Format.fprintf f " %s (%s : %s) : Yojson.Safe.t =@ " (type_name name) nm
    (type_name name);
  match sh with
  | Blob -> Format.fprintf f "`String (Base64.encode_string %s)" nm
  | Boolean -> Format.fprintf f "`Bool %s" nm
  | String -> Format.fprintf f "`String %s" nm
  | Enum l ->
      Format.fprintf f "`String @[<hv1>(match %s with@ %a)@]" nm
        (Format.pp_print_list
           ~pp_sep:(fun f () -> Format.fprintf f "@ ")
           (fun f (nm, _, traits) ->
             Format.fprintf f "| %s -> \"%s\"" (constr_name nm)
               (try
                  Yojson.Safe.Util.to_string
                    (List.assoc "smithy.api#enumValue" traits)
                with Not_found -> nm)))
        l
  | Integer -> Format.fprintf f "`Intlit (Int32.to_string %s)" nm
  | Long -> Format.fprintf f "`Intlit (Int64.to_string %s)" nm
  | Float | Double -> Format.fprintf f "`Float %s" nm
  | Timestamp ->
      Format.fprintf f "`Float (CalendarLib.Calendar.to_unixfloat %s)" nm
  | Document -> Format.fprintf f "%s" nm
  | List id ->
      let sparse = List.mem_assoc "smithy.api#sparse" traits in
      Format.fprintf f "`List (List.map %a %s)" (converter ~sparse) id nm
  | Map (_key, value) ->
      let sparse = List.mem_assoc "smithy.api#sparse" traits in
      Format.fprintf f "`Assoc (StringMap.bindings (StringMap.map %a %s))"
        (converter ~sparse) value nm
  | Structure [] -> Format.fprintf f "`Assoc []"
  | Structure l ->
      Format.fprintf f
        "@[<hv>match %s with@ | @[<hv>{%a }@] ->@;<1 2>`Assoc @[<hv>[%a ]@]@]"
        nm
        (Format.pp_print_list
           ~pp_sep:(fun f () -> Format.fprintf f "@,;")
           (fun f (nm, _, _) ->
             Format.fprintf f " %s = %s" (field_name nm) (field_name nm ^ "'")))
        l
        (Format.pp_print_list
           ~pp_sep:(fun f () -> Format.fprintf f "@,;")
           (fun f ((nm, typ, _) as field) ->
             let optional = optional_member field in
             Format.fprintf f " (\"%s\", %a %s)" nm
               (converter ~sparse:optional)
               typ
               (field_name nm ^ "'")))
        l
  | Union l ->
      Format.fprintf f "`Assoc @[<hv1>[match %s with@ %a]@]" nm
        (Format.pp_print_list
           ~pp_sep:(fun f () -> Format.fprintf f "@ ")
           (fun f (nm, typ, _traits) ->
             Format.fprintf f "| %s x -> (\"%s\", %a x)" (constr_name nm) nm
               (converter ~sparse:false) typ))
        l
  | Service | Resource | Operation -> assert false

let print_to_json f shs =
  Format.fprintf f "@[<hv2>module To_Json = struct@ ";
  Format.fprintf f "@[<hv2>let rec";
  Format.pp_print_list
    ~pp_sep:(fun f () -> Format.fprintf f "@]@ @[<hv2>and")
    to_json f
    (IdMap.bindings
       (IdMap.filter
          (fun _ (typ, _) ->
            match typ with Service | Operation | Resource -> false | _ -> true)
          shs));
  Format.fprintf f "@]@;<1 -2>end@]@."

let compile dir f =
  let shs = parse (Filename.concat dir f) in
  let ch =
    open_out
      (Filename.concat "generated"
         (String.concat "_"
            (String.split_on_char '-' (Filename.chop_suffix f ".json"))
         ^ ".ml"))
  in
  let f = Format.formatter_of_out_channel ch in
  Format.fprintf f
    "module StringMap = Map.Make (struct type t = string let compare = compare \
     end)@.";
  print_types f shs;
  print_constructors f shs;
  print_to_json f shs;
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
    let shs = parse (Filename.concat dir f) in
    Format.printf
      "module StringMap = Map.Make (struct type t = string let compare = \
       compare end)@.";
    List.iteri
      (fun i sh -> Format.printf "%a" (print_type ~first:(i = 0)) sh)
      shs
*)
