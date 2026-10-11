fn main() {
    // 文法变更时重跑本脚本
    println!("cargo:rerun-if-changed=src/parser/sql.lalrpop");
    // in=src、out=$OUT_DIR：生成物落在 OUT_DIR/parser/sql.rs，供 lalrpop_mod! 引用
    lalrpop::Configuration::new()
        .use_cargo_dir_conventions()
        .process()
        .unwrap();
}
