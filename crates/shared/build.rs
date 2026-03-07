fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    let out_dir = std::path::Path::new("src/generated");
    std::fs::create_dir_all(out_dir)?;
    prost_build::Config::new()
        .out_dir(out_dir)
        .type_attribute(".", "#[derive(Debug)]")
        .compile_protos(
            &[
                "proto/click.proto",
                "proto/view.proto",
                "proto/purchase.proto",
            ],
            &["proto/"],
        )?;
    Ok(())
}