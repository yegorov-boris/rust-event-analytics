fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    prost_build::compile_protos(
        &[
            "proto/click.proto",
            "proto/view.proto",
            "proto/purchase.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}