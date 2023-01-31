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
