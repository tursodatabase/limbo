mod args;
use args::{RegisterExtensionInput, ScalarInfo};
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput, ItemFn};
extern crate proc_macro;
use proc_macro::{token_stream::IntoIter, Group, TokenStream, TokenTree};
use std::collections::HashMap;

/// A procedural macro that derives a `Description` trait for enums.
/// This macro extracts documentation comments (specified with `/// Description...`) for enum variants
/// and generates an implementation for `get_description`, which returns the associated description.
#[proc_macro_derive(Description, attributes(desc))]
pub fn derive_description_from_doc(item: TokenStream) -> TokenStream {
    // Convert the TokenStream into an iterator of TokenTree
    let mut tokens = item.into_iter();

    let mut enum_name = String::new();

    // Vector to store enum variants and their associated payloads (if any)
    let mut enum_variants: Vec<(String, Option<String>)> = Vec::<(String, Option<String>)>::new();

    // HashMap to store descriptions associated with each enum variant
    let mut variant_description_map: HashMap<String, String> = HashMap::new();

    // Parses the token stream to extract the enum name and its variants
    while let Some(token) = tokens.next() {
        match token {
            TokenTree::Ident(ident) if ident.to_string() == "enum" => {
                // Get the enum name
                if let Some(TokenTree::Ident(name)) = tokens.next() {
                    enum_name = name.to_string();
                }
            }
            TokenTree::Group(group) => {
                let mut group_tokens_iter: IntoIter = group.stream().into_iter();

                let mut last_seen_desc: Option<String> = None;
                while let Some(token) = group_tokens_iter.next() {
                    match token {
                        TokenTree::Punct(punct) => {
                            if punct.to_string() == "#" {
                                last_seen_desc = process_description(&mut group_tokens_iter);
                            }
                        }
                        TokenTree::Ident(ident) => {
                            // Capture the enum variant name and associate it with its description
                            let ident_str = ident.to_string();
                            if let Some(desc) = &last_seen_desc {
                                variant_description_map.insert(ident_str.clone(), desc.clone());
                            }
                            enum_variants.push((ident_str, None));
                            last_seen_desc = None;
                        }
                        TokenTree::Group(group) => {
                            // Capture payload information for the current enum variant
                            if let Some(last_variant) = enum_variants.last_mut() {
                                last_variant.1 = Some(process_payload(group));
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    generate_get_description(enum_name, &variant_description_map, enum_variants)
}

/// Processes a Rust docs to extract the description string.
fn process_description(token_iter: &mut IntoIter) -> Option<String> {
    if let Some(TokenTree::Group(doc_group)) = token_iter.next() {
        let mut doc_group_iter = doc_group.stream().into_iter();
        // Skip the `desc` and `(` tokens to reach the actual description
        doc_group_iter.next();
        doc_group_iter.next();
        if let Some(TokenTree::Literal(description)) = doc_group_iter.next() {
            return Some(description.to_string());
        }
    }
    None
}

/// Processes the payload of an enum variant to extract variable names (ignoring types).
fn process_payload(payload_group: Group) -> String {
    let payload_group_iter = payload_group.stream().into_iter();
    let mut variable_name_list = String::from("");
    let mut is_variable_name = true;
    for token in payload_group_iter {
        match token {
            TokenTree::Ident(ident) => {
                if is_variable_name {
                    variable_name_list.push_str(&format!("{},", ident));
                }
                is_variable_name = false;
            }
            TokenTree::Punct(punct) => {
                if punct.to_string() == "," {
                    is_variable_name = true;
                }
            }
            _ => {}
        }
    }
    format!("{{ {} }}", variable_name_list).to_string()
}
/// Generates the `get_description` implementation for the processed enum.
fn generate_get_description(
    enum_name: String,
    variant_description_map: &HashMap<String, String>,
    enum_variants: Vec<(String, Option<String>)>,
) -> TokenStream {
    let mut all_enum_arms = String::from("");
    for (variant, payload) in enum_variants {
        let payload = payload.unwrap_or("".to_string());
        let desc;
        if let Some(description) = variant_description_map.get(&variant) {
            desc = format!("Some({})", description);
        } else {
            desc = "None".to_string();
        }
        all_enum_arms.push_str(&format!(
            "{}::{} {} => {},\n",
            enum_name, variant, payload, desc
        ));
    }

    let enum_impl = format!(
        "impl {}  {{ 
     pub fn get_description(&self) -> Option<&str> {{
     match self {{
     {}
     }}
     }}
     }}",
        enum_name, all_enum_arms
    );
    enum_impl.parse().unwrap()
}

/// Declare a scalar function for your extension. This requires the name:
/// #[scalar(name = "example")] of what you wish to call your function with.
/// ```ignore
/// use limbo_ext::{scalar, Value};
/// #[scalar(name = "double", alias = "twice")] // you can provide an <optional> alias
/// fn double(args: &[Value]) -> Value {
///       let arg = args.get(0).unwrap();
///       match arg.value_type() {
///           ValueType::Float => {
///               let val = arg.to_float().unwrap();
///               Value::from_float(val * 2.0)
///           }
///           ValueType::Integer => {
///               let val = arg.to_integer().unwrap();
///               Value::from_integer(val * 2)
///           }
///       }
///   } else {
///       Value::null()
///   }
/// }
/// ```
#[proc_macro_attribute]
pub fn scalar(attr: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as ItemFn);
    let fn_name = &ast.sig.ident;
    let args_variable = &ast.sig.inputs.first();
    let mut args_variable_name = None;
    if let Some(syn::FnArg::Typed(syn::PatType { pat, .. })) = args_variable {
        if let syn::Pat::Ident(ident) = &**pat {
            args_variable_name = Some(ident.ident.clone());
        }
    }
    let scalar_info = parse_macro_input!(attr as ScalarInfo);
    let name = &scalar_info.name;
    let register_fn_name = format_ident!("register_{}", fn_name);
    let args_variable_name =
        format_ident!("{}", args_variable_name.unwrap_or(format_ident!("args")));
    let fn_body = &ast.block;
    let alias_check = if let Some(alias) = &scalar_info.alias {
        quote! {
            let Ok(alias_c_name) = ::std::ffi::CString::new(#alias) else {
                return ::limbo_ext::ResultCode::Error;
            };
            (api.register_scalar_function)(
                api.ctx,
                alias_c_name.as_ptr(),
                #fn_name,
            );
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #[no_mangle]
        pub unsafe extern "C" fn #register_fn_name(
            api: *const ::limbo_ext::ExtensionApi
        ) -> ::limbo_ext::ResultCode {
            if api.is_null() {
                return ::limbo_ext::ResultCode::Error;
            }
            let api = unsafe { &*api };
            let Ok(c_name) = ::std::ffi::CString::new(#name) else {
                return ::limbo_ext::ResultCode::Error;
            };
            (api.register_scalar_function)(
                api.ctx,
                c_name.as_ptr(),
                #fn_name,
            );
            #alias_check
            ::limbo_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #fn_name(
            argc: i32,
            argv: *const ::limbo_ext::Value
        ) -> ::limbo_ext::Value {
            let #args_variable_name = if argv.is_null() || argc <= 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(argv, argc as usize) }
            };
            #fn_body
        }
    };

    TokenStream::from(expanded)
}

/// Define an aggregate function for your extension by deriving
/// AggregateDerive on a struct that implements the AggFunc trait.
/// ```ignore
/// use limbo_ext::{register_extension, Value, AggregateDerive, AggFunc};
///
///#[derive(AggregateDerive)]
///struct SumPlusOne;
///
///impl AggFunc for SumPlusOne {
///   type State = i64;
///   type Error = &'static str;
///   const NAME: &'static str = "sum_plus_one";
///   const ARGS: i32 = 1;
///   fn step(state: &mut Self::State, args: &[Value]) {
///      let Some(val) = args[0].to_integer() else {
///        return;
///     };
///     *state += val;
///     }
///     fn finalize(state: Self::State) -> Result<Value, Self::Error> {
///        Ok(Value::from_integer(state + 1))
///     }
///}
/// ```
#[proc_macro_derive(AggregateDerive)]
pub fn derive_agg_func(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let step_fn_name = format_ident!("{}_step", struct_name);
    let finalize_fn_name = format_ident!("{}_finalize", struct_name);
    let init_fn_name = format_ident!("{}_init", struct_name);
    let register_fn_name = format_ident!("register_{}", struct_name);

    let expanded = quote! {
        impl #struct_name {
            #[no_mangle]
            pub extern "C" fn #init_fn_name() -> *mut ::limbo_ext::AggCtx {
                let state = ::std::boxed::Box::new(<#struct_name as ::limbo_ext::AggFunc>::State::default());
                let ctx = ::std::boxed::Box::new(::limbo_ext::AggCtx {
                    state: ::std::boxed::Box::into_raw(state) as *mut ::std::os::raw::c_void,
                });
                ::std::boxed::Box::into_raw(ctx)
            }

            #[no_mangle]
            pub extern "C" fn #step_fn_name(
                ctx: *mut ::limbo_ext::AggCtx,
                argc: i32,
                argv: *const ::limbo_ext::Value,
            ) {
                unsafe {
                    let ctx = &mut *ctx;
                    let state = &mut *(ctx.state as *mut <#struct_name as ::limbo_ext::AggFunc>::State);
                    let args = ::std::slice::from_raw_parts(argv, argc as usize);
                    <#struct_name as ::limbo_ext::AggFunc>::step(state, args);
                }
            }

            #[no_mangle]
            pub extern "C" fn #finalize_fn_name(
                ctx: *mut ::limbo_ext::AggCtx
            ) -> ::limbo_ext::Value {
                unsafe {
                    let ctx = &mut *ctx;
                    let state = ::std::boxed::Box::from_raw(ctx.state as *mut <#struct_name as ::limbo_ext::AggFunc>::State);
                    match <#struct_name as ::limbo_ext::AggFunc>::finalize(*state) {
                        Ok(val) => val,
                        Err(e) => {
                            ::limbo_ext::Value::error_with_message(e.to_string())
                        }
                    }
                }
            }

            #[no_mangle]
            pub unsafe extern "C" fn #register_fn_name(
                api: *const ::limbo_ext::ExtensionApi
            ) -> ::limbo_ext::ResultCode {
                if api.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }

                let api = &*api;
                let name_str = #struct_name::NAME;
                let c_name = match ::std::ffi::CString::new(name_str) {
                    Ok(cname) => cname,
                    Err(_) => return ::limbo_ext::ResultCode::Error,
                };

                (api.register_aggregate_function)(
                    api.ctx,
                    c_name.as_ptr(),
                    #struct_name::ARGS,
                    #struct_name::#init_fn_name
                        as ::limbo_ext::InitAggFunction,
                    #struct_name::#step_fn_name
                        as ::limbo_ext::StepFunction,
                    #struct_name::#finalize_fn_name
                        as ::limbo_ext::FinalizeFunction,
                )
            }
        }
    };

    TokenStream::from(expanded)
}

/// Macro to derive a VTabModule for your extension. This macro will generate
/// the necessary functions to register your module with core. You must implement
/// the VTabModule trait for your struct, and the VTabCursor trait for your cursor.
/// ```ignore
///#[derive(Debug, VTabModuleDerive)]
///struct CsvVTab;
///impl VTabModule for CsvVTab {
///    type VCursor = CsvCursor;
///    const NAME: &'static str = "csv_data";
///
///    /// Declare the schema for your virtual table
///    fn create_schema(args: &[&str]) -> &'static str {
///        let sql = "CREATE TABLE csv_data(
///            name TEXT,
///            age TEXT,
///            city TEXT
///        )"
///    }
///    /// Open the virtual table and return a cursor
///  fn open() -> Self::VCursor {
///       let csv_content = fs::read_to_string("data.csv").unwrap_or_default();
///       let rows: Vec<Vec<String>> = csv_content
///           .lines()
///           .skip(1)
///           .map(|line| {
///               line.split(',')
///                   .map(|s| s.trim().to_string())
///                   .collect()
///           })
///           .collect();
///       CsvCursor { rows, index: 0 }
///   }
///   /// Filter the virtual table based on arguments (omitted here for simplicity)
///   fn filter(_cursor: &mut Self::VCursor, _arg_count: i32, _args: &[Value]) -> ResultCode {
///       ResultCode::OK
///   }
///   /// Return the value for a given column index
///   fn column(cursor: &Self::VCursor, idx: u32) -> Value {
///      cursor.column(idx)
///  }
///  /// Move the cursor to the next row
///  fn next(cursor: &mut Self::VCursor) -> ResultCode {
///      if cursor.index < cursor.rows.len() - 1 {
///          cursor.index += 1;
///          ResultCode::OK
///      } else {
///          ResultCode::EOF
///      }
///  }
///  fn eof(cursor: &Self::VCursor) -> bool {
///      cursor.index >= cursor.rows.len()
///  }
///
/// /// **Optional** methods for non-readonly tables:
///
///  /// Update the row with the provided values, return the new rowid
///  fn update(&mut self, rowid: i64, args: &[Value]) -> Result<Option<i64>, Self::Error> {
///      Ok(None)// return Ok(None) for read-only
///  }
///  /// Insert a new row with the provided values, return the new rowid
///  fn insert(&mut self, args: &[Value]) -> Result<(), Self::Error> {
///      Ok(()) //
///  }
///  /// Delete the row with the provided rowid
///  fn delete(&mut self, rowid: i64) -> Result<(), Self::Error> {
///    Ok(())
/// }
///
///  #[derive(Debug)]
/// struct CsvCursor {
///   rows: Vec<Vec<String>>,
///   index: usize,
///
/// impl CsvCursor {
///   /// Returns the value for a given column index.
///   fn column(&self, idx: u32) -> Result<Value, Self::Error> {
///       let row = &self.rows[self.index];
///       if (idx as usize) < row.len() {
///           Value::from_text(&row[idx as usize])
///       } else {
///           Value::null()
///       }
///   }
/// // Implement the VTabCursor trait for your virtual cursor
/// impl VTabCursor for CsvCursor {
///   fn next(&mut self) -> ResultCode {
///       Self::next(self)
///   }
///  fn eof(&self) -> bool {
///      self.index >= self.rows.len()
///  }
///  fn column(&self, idx: u32) -> Value {
///      self.column(idx)
///  }
///  fn rowid(&self) -> i64 {
///      self.index as i64
///  }
///
#[proc_macro_derive(VTabModuleDerive)]
pub fn derive_vtab_module(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let register_fn_name = format_ident!("register_{}", struct_name);
    let create_schema_fn_name = format_ident!("create_schema_{}", struct_name);
    let open_fn_name = format_ident!("open_{}", struct_name);
    let filter_fn_name = format_ident!("filter_{}", struct_name);
    let column_fn_name = format_ident!("column_{}", struct_name);
    let next_fn_name = format_ident!("next_{}", struct_name);
    let eof_fn_name = format_ident!("eof_{}", struct_name);
    let update_fn_name = format_ident!("update_{}", struct_name);
    let rowid_fn_name = format_ident!("rowid_{}", struct_name);

    let expanded = quote! {
        impl #struct_name {
            #[no_mangle]
            unsafe extern "C" fn #create_schema_fn_name(
                argv: *const ::limbo_ext::Value, argc: i32
            ) -> *mut ::std::ffi::c_char {
                let args = if argv.is_null() {
                    &Vec::new()
                } else {
                    ::std::slice::from_raw_parts(argv, argc as usize)
                };
                let sql = <#struct_name as ::limbo_ext::VTabModule>::create_schema(&args);
                ::std::ffi::CString::new(sql).unwrap().into_raw()
            }

            #[no_mangle]
            unsafe extern "C" fn #open_fn_name(ctx: *const ::std::ffi::c_void, conn: *mut ::limbo_ext::Conn) -> *const ::std::ffi::c_void {
                if ctx.is_null() || conn.is_null() {
                      return ::std::ptr::null();
                }
                let conn = ::std::rc::Rc::new(::limbo_ext::Connection::new(conn));
                if let Ok(cursor) = <#struct_name as ::limbo_ext::VTabModule>::open(ctx, conn) {
                    return ::std::boxed::Box::into_raw(::std::boxed::Box::new(cursor)) as *const ::std::ffi::c_void;
                } else {
                    return ::std::ptr::null();
                }
            }

            #[no_mangle]
            unsafe extern "C" fn #filter_fn_name(
                cursor: *const ::std::ffi::c_void,
                argc: i32,
                argv: *const ::limbo_ext::Value,
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = unsafe { &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor) };
                let args = ::std::slice::from_raw_parts(argv, argc as usize);
                <#struct_name as ::limbo_ext::VTabModule>::filter(cursor, args)
            }

            #[no_mangle]
            unsafe extern "C" fn #column_fn_name(
                cursor: *const ::std::ffi::c_void,
                idx: u32,
            ) -> ::limbo_ext::Value {
                if cursor.is_null() {
                    return ::limbo_ext::Value::error(::limbo_ext::ResultCode::Error);
                }
                let cursor = unsafe { &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor) };
                match <#struct_name as ::limbo_ext::VTabModule>::column(cursor, idx) {
                    Ok(val) => val,
                    Err(e) => ::limbo_ext::Value::error_with_message(e.to_string())
                }
            }

            #[no_mangle]
            unsafe extern "C" fn #next_fn_name(
                cursor: *const ::std::ffi::c_void,
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor);
                <#struct_name as ::limbo_ext::VTabModule>::next(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #eof_fn_name(
                cursor: *const ::std::ffi::c_void,
            ) -> bool {
                if cursor.is_null() {
                    return true;
                }
                let cursor = &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor);
                <#struct_name as ::limbo_ext::VTabModule>::eof(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #update_fn_name(
                vtab: *const ::std::ffi::c_void,
                argc: i32,
                argv: *const ::limbo_ext::Value,
                p_out_rowid: *mut i64,
            ) -> ::limbo_ext::ResultCode {
                if vtab.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }

                let vtab = &mut *(vtab as *mut #struct_name);
                let args = ::std::slice::from_raw_parts(argv, argc as usize);

                let old_rowid = match args.get(0).map(|v| v.value_type()) {
                    Some(::limbo_ext::ValueType::Integer) => args.get(0).unwrap().to_integer(),
                    _ => None,
                };
                let new_rowid = match args.get(1).map(|v| v.value_type()) {
                    Some(::limbo_ext::ValueType::Integer) => args.get(1).unwrap().to_integer(),
                    _ => None,
                };
                let columns = &args[2..];
                match (old_rowid, new_rowid) {
                    // DELETE: old_rowid provided, no new_rowid
                    (Some(old), None) => {
                     if <#struct_name as VTabModule>::delete(vtab, old).is_err() {
                            return ::limbo_ext::ResultCode::Error;
                      }
                            return ::limbo_ext::ResultCode::OK;
                    }
                    // UPDATE: old_rowid provided and new_rowid may exist
                    (Some(old), Some(new)) => {
                        if <#struct_name as VTabModule>::update(vtab, old, &columns).is_err() {
                            return ::limbo_ext::ResultCode::Error;
                        }
                        return ::limbo_ext::ResultCode::OK;
                    }
                    // INSERT: no old_rowid (old_rowid = None)
                    (None, _) => {
                        if let Ok(rowid) = <#struct_name as VTabModule>::insert(vtab, &columns) {
                            if !p_out_rowid.is_null() {
                                *p_out_rowid = rowid;
                                 return ::limbo_ext::ResultCode::RowID;
                            }
                            return ::limbo_ext::ResultCode::OK;
                        }
                    }
                }
                return ::limbo_ext::ResultCode::Error;
            }

            #[no_mangle]
            pub unsafe extern "C" fn #rowid_fn_name(ctx: *const ::std::ffi::c_void) -> i64 {
                if ctx.is_null() {
                    return -1;
                }
                let cursor = &*(ctx as *const <#struct_name as ::limbo_ext::VTabModule>::VCursor);
                <<#struct_name as ::limbo_ext::VTabModule>::VCursor as ::limbo_ext::VTabCursor>::rowid(cursor)
            }

            #[no_mangle]
            pub unsafe extern "C" fn #register_fn_name(
                api: *mut ::limbo_ext::ExtensionApi
            ) -> ::limbo_ext::ResultCode {
                if api.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let api = &mut *api;
                // establish connection on vtab module registration
                let connection = unsafe { (api.connect)(api.ctx) };
                api.conn = connection;
                let name = <#struct_name as ::limbo_ext::VTabModule>::NAME;
                let name_c = ::std::ffi::CString::new(name).unwrap().into_raw() as *const ::std::ffi::c_char;
                let table_instance = ::std::boxed::Box::into_raw(::std::boxed::Box::new(#struct_name::default()));
                let conn = (api.connect)(api.ctx);
                let module = ::limbo_ext::VTabModuleImpl {
                    ctx: table_instance as *const ::std::ffi::c_void,
                    conn,
                    name: name_c,
                    conn: api.conn,
                    create_schema: Self::#create_schema_fn_name,
                    open: Self::#open_fn_name,
                    filter: Self::#filter_fn_name,
                    column: Self::#column_fn_name,
                    next: Self::#next_fn_name,
                    eof: Self::#eof_fn_name,
                    update: Self::#update_fn_name,
                    rowid: Self::#rowid_fn_name,
                };
                (api.register_module)(api.ctx, name_c, module, <#struct_name as ::limbo_ext::VTabModule>::VTAB_KIND)
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(VfsDerive)]
pub fn derive_vfs_module(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);
    let struct_name = &derive_input.ident;
    let register_fn_name = format_ident!("register_{}", struct_name);
    let register_static = format_ident!("register_static_{}", struct_name);
    let open_fn_name = format_ident!("{}_open", struct_name);
    let close_fn_name = format_ident!("{}_close", struct_name);
    let read_fn_name = format_ident!("{}_read", struct_name);
    let write_fn_name = format_ident!("{}_write", struct_name);
    let lock_fn_name = format_ident!("{}_lock", struct_name);
    let unlock_fn_name = format_ident!("{}_unlock", struct_name);
    let sync_fn_name = format_ident!("{}_sync", struct_name);
    let size_fn_name = format_ident!("{}_size", struct_name);
    let run_once_fn_name = format_ident!("{}_run_once", struct_name);
    let generate_random_number_fn_name = format_ident!("{}_generate_random_number", struct_name);
    let get_current_time_fn_name = format_ident!("{}_get_current_time", struct_name);

    let expanded = quote! {
        #[allow(non_snake_case)]
        pub unsafe extern "C" fn #register_static() -> *const ::limbo_ext::VfsImpl {
            let ctx = #struct_name::default();
            let ctx = ::std::boxed::Box::into_raw(::std::boxed::Box::new(ctx)) as *const ::std::ffi::c_void;
            let name = ::std::ffi::CString::new(<#struct_name as ::limbo_ext::VfsExtension>::NAME).unwrap().into_raw();
            let vfs_mod = ::limbo_ext::VfsImpl {
                vfs: ctx,
                name,
                open: #open_fn_name,
                close: #close_fn_name,
                read: #read_fn_name,
                write: #write_fn_name,
                lock: #lock_fn_name,
                unlock: #unlock_fn_name,
                sync: #sync_fn_name,
                size: #size_fn_name,
                run_once: #run_once_fn_name,
                gen_random_number: #generate_random_number_fn_name,
                current_time: #get_current_time_fn_name,
            };
            ::std::boxed::Box::into_raw(::std::boxed::Box::new(vfs_mod)) as *const ::limbo_ext::VfsImpl
        }

        #[no_mangle]
        pub unsafe extern "C" fn #register_fn_name(api: &::limbo_ext::ExtensionApi) -> ::limbo_ext::ResultCode {
            let ctx = #struct_name::default();
            let ctx = ::std::boxed::Box::into_raw(::std::boxed::Box::new(ctx)) as *const ::std::ffi::c_void;
            let name = ::std::ffi::CString::new(<#struct_name as ::limbo_ext::VfsExtension>::NAME).unwrap().into_raw();
            let vfs_mod = ::limbo_ext::VfsImpl {
                vfs: ctx,
                name,
                open: #open_fn_name,
                close: #close_fn_name,
                read: #read_fn_name,
                write: #write_fn_name,
                lock: #lock_fn_name,
                unlock: #unlock_fn_name,
                sync: #sync_fn_name,
                size: #size_fn_name,
                run_once: #run_once_fn_name,
                gen_random_number: #generate_random_number_fn_name,
                current_time: #get_current_time_fn_name,
            };
            let vfsimpl = ::std::boxed::Box::into_raw(::std::boxed::Box::new(vfs_mod)) as *const ::limbo_ext::VfsImpl;
            (api.register_vfs)(name, vfsimpl)
        }

        #[no_mangle]
        pub unsafe extern "C" fn #open_fn_name(
            ctx: *const ::std::ffi::c_void,
            path: *const ::std::ffi::c_char,
            flags: i32,
            direct: bool,
        ) -> *const ::std::ffi::c_void {
            let ctx = &*(ctx as *const ::limbo_ext::VfsImpl);
            let Ok(path_str) = ::std::ffi::CStr::from_ptr(path).to_str() else {
                  return ::std::ptr::null_mut();
            };
            let vfs = &*(ctx.vfs as *const #struct_name);
            let Ok(file_handle) = <#struct_name as ::limbo_ext::VfsExtension>::open_file(vfs, path_str, flags, direct) else {
                return ::std::ptr::null();
            };
            let boxed = ::std::boxed::Box::into_raw(::std::boxed::Box::new(file_handle)) as *const ::std::ffi::c_void;
            let Ok(vfs_file) = ::limbo_ext::VfsFileImpl::new(boxed, ctx) else {
                return ::std::ptr::null();
            };
            ::std::boxed::Box::into_raw(::std::boxed::Box::new(vfs_file)) as *const ::std::ffi::c_void
        }

        #[no_mangle]
        pub unsafe extern "C" fn #close_fn_name(file_ptr: *const ::std::ffi::c_void) -> ::limbo_ext::ResultCode {
            if file_ptr.is_null() {
                return ::limbo_ext::ResultCode::Error;
            }
            let vfs_file: &mut ::limbo_ext::VfsFileImpl = &mut *(file_ptr as *mut ::limbo_ext::VfsFileImpl);
            let vfs_instance = &*(vfs_file.vfs as *const #struct_name);

            // this time we need to own it so we can drop it
            let file: ::std::boxed::Box<<#struct_name as ::limbo_ext::VfsExtension>::File> =
             ::std::boxed::Box::from_raw(vfs_file.file as *mut <#struct_name as ::limbo_ext::VfsExtension>::File);
            if let Err(e) = <#struct_name as ::limbo_ext::VfsExtension>::close(vfs_instance, *file) {
                return e;
            }
            ::limbo_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #read_fn_name(file_ptr: *const ::std::ffi::c_void, buf: *mut u8, count: usize, offset: i64) -> i32 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::limbo_ext::VfsFileImpl = &mut *(file_ptr as *mut ::limbo_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::limbo_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::limbo_ext::VfsExtension>::File);
            match <#struct_name as ::limbo_ext::VfsExtension>::File::read(file, ::std::slice::from_raw_parts_mut(buf, count), count, offset) {
                Ok(n) => n,
                Err(_) => -1,
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn #run_once_fn_name(ctx: *const ::std::ffi::c_void) -> ::limbo_ext::ResultCode {
            if ctx.is_null() {
                return ::limbo_ext::ResultCode::Error;
            }
            let ctx = &mut *(ctx as *mut #struct_name);
            if let Err(e) = <#struct_name as ::limbo_ext::VfsExtension>::run_once(ctx) {
                return e;
            }
            ::limbo_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #write_fn_name(file_ptr: *const ::std::ffi::c_void, buf: *const u8, count: usize, offset: i64) -> i32 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::limbo_ext::VfsFileImpl = &mut *(file_ptr as *mut ::limbo_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::limbo_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::limbo_ext::VfsExtension>::File);
            match <#struct_name as ::limbo_ext::VfsExtension>::File::write(file, ::std::slice::from_raw_parts(buf, count), count, offset) {
                Ok(n) => n,
                Err(_) => -1,
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn #lock_fn_name(file_ptr: *const ::std::ffi::c_void, exclusive: bool) -> ::limbo_ext::ResultCode {
            if file_ptr.is_null() {
                return ::limbo_ext::ResultCode::Error;
            }
            let vfs_file: &mut ::limbo_ext::VfsFileImpl = &mut *(file_ptr as *mut ::limbo_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::limbo_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::limbo_ext::VfsExtension>::File);
            if let Err(e) = <#struct_name as ::limbo_ext::VfsExtension>::File::lock(file, exclusive) {
                return e;
            }
            ::limbo_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #unlock_fn_name(file_ptr: *const ::std::ffi::c_void) -> ::limbo_ext::ResultCode {
            if file_ptr.is_null() {
                return ::limbo_ext::ResultCode::Error;
            }
            let vfs_file: &mut ::limbo_ext::VfsFileImpl = &mut *(file_ptr as *mut ::limbo_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::limbo_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::limbo_ext::VfsExtension>::File);
            if let Err(e) = <#struct_name as ::limbo_ext::VfsExtension>::File::unlock(file) {
                return e;
            }
            ::limbo_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #sync_fn_name(file_ptr: *const ::std::ffi::c_void) -> i32 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::limbo_ext::VfsFileImpl = &mut *(file_ptr as *mut ::limbo_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::limbo_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::limbo_ext::VfsExtension>::File);
            if <#struct_name as ::limbo_ext::VfsExtension>::File::sync(file).is_err() {
                return -1;
            }
            0
        }

        #[no_mangle]
        pub unsafe extern "C" fn #size_fn_name(file_ptr: *const ::std::ffi::c_void) -> i64 {
            if file_ptr.is_null() {
                return -1;
            }
            let vfs_file: &mut ::limbo_ext::VfsFileImpl = &mut *(file_ptr as *mut ::limbo_ext::VfsFileImpl);
            let file: &mut <#struct_name as ::limbo_ext::VfsExtension>::File =
                &mut *(vfs_file.file as *mut <#struct_name as ::limbo_ext::VfsExtension>::File);
            <#struct_name as ::limbo_ext::VfsExtension>::File::size(file)
        }

        #[no_mangle]
        pub unsafe extern "C" fn #generate_random_number_fn_name() -> i64 {
            let obj = #struct_name::default();
            <#struct_name as ::limbo_ext::VfsExtension>::generate_random_number(&obj)
        }

        #[no_mangle]
        pub unsafe extern "C" fn #get_current_time_fn_name() -> *const ::std::ffi::c_char {
            let obj = #struct_name::default();
            let time = <#struct_name as ::limbo_ext::VfsExtension>::get_current_time(&obj);
            // release ownership of the string to core
            ::std::ffi::CString::new(time).unwrap().into_raw() as *const ::std::ffi::c_char
        }
    };

    TokenStream::from(expanded)
}

/// Register your extension with 'core' by providing the relevant functions
///```ignore
///use limbo_ext::{register_extension, scalar, Value, AggregateDerive, AggFunc};
///
/// register_extension!{ scalars: { return_one }, aggregates: { SumPlusOne } }
///
///#[scalar(name = "one")]
///fn return_one(args: &[Value]) -> Value {
///  return Value::from_integer(1);
///}
///
///#[derive(AggregateDerive)]
///struct SumPlusOne;
///
///impl AggFunc for SumPlusOne {
///   type State = i64;
///   const NAME: &'static str = "sum_plus_one";
///   const ARGS: i32 = 1;
///
///   fn step(state: &mut Self::State, args: &[Value]) {
///      let Some(val) = args[0].to_integer() else {
///        return;
///      };
///      *state += val;
///     }
///
///     fn finalize(state: Self::State) -> Value {
///        Value::from_integer(state + 1)
///     }
///}
///
/// ```
#[proc_macro]
pub fn register_extension(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as RegisterExtensionInput);
    let RegisterExtensionInput {
        aggregates,
        scalars,
        vtabs,
        vfs,
    } = input_ast;

    let scalar_calls = scalars.iter().map(|scalar_ident| {
        let register_fn =
            syn::Ident::new(&format!("register_{}", scalar_ident), scalar_ident.span());
        quote! {
            {
                let result = unsafe { #register_fn(api)};
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });

    let aggregate_calls = aggregates.iter().map(|agg_ident| {
        let register_fn = syn::Ident::new(&format!("register_{}", agg_ident), agg_ident.span());
        quote! {
            {
                let result = unsafe{ #agg_ident::#register_fn(api)};
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });
    let vtab_calls = vtabs.iter().map(|vtab_ident| {
        let register_fn = syn::Ident::new(&format!("register_{}", vtab_ident), vtab_ident.span());
        quote! {
            {
                let result = unsafe{ #vtab_ident::#register_fn(api)};
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });
    let vfs_calls = vfs.iter().map(|vfs_ident| {
        let register_fn = syn::Ident::new(&format!("register_{}", vfs_ident), vfs_ident.span());
        quote! {
            {
                let result = unsafe { #register_fn(api) };
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });
    let static_vfs = vfs.iter().map(|vfs_ident| {
        let static_register =
            syn::Ident::new(&format!("register_static_{}", vfs_ident), vfs_ident.span());
        quote! {
            {
                    let result = api.add_builtin_vfs(unsafe { #static_register()});
                    if !result.is_ok() {
                        return result;
                }
            }
        }
    });
    let static_aggregates = aggregate_calls.clone();
    let static_scalars = scalar_calls.clone();
    let static_vtabs = vtab_calls.clone();

    let expanded = quote! {
    #[cfg(not(target_family = "wasm"))]
    #[cfg(not(feature = "static"))]
    #[global_allocator]
    static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

            #[cfg(feature = "static")]
            pub unsafe extern "C" fn register_extension_static(api: &mut ::limbo_ext::ExtensionApi) -> ::limbo_ext::ResultCode {
                let api = unsafe { &*api as *const ::limbo_ext::ExtensionApi } as *mut ::limbo_ext::ExtensionApi;
                #(#static_scalars)*

                #(#static_aggregates)*

                #(#static_vtabs)*

                #[cfg(not(target_family = "wasm"))]
                #(#static_vfs)*

                ::limbo_ext::ResultCode::OK
              }

            #[cfg(not(feature = "static"))]
            #[no_mangle]
            pub unsafe extern "C" fn register_extension(api: &::limbo_ext::ExtensionApi) -> ::limbo_ext::ResultCode {
                let api = unsafe { &*api as *const ::limbo_ext::ExtensionApi } as *mut ::limbo_ext::ExtensionApi;
                #(#scalar_calls)*

                #(#aggregate_calls)*

                #(#vtab_calls)*

                #(#vfs_calls)*

                ::limbo_ext::ResultCode::OK
            }
        };

    TokenStream::from(expanded)
}
