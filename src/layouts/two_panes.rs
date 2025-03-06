use std::fmt::Display;

use dioxus::prelude::*;

use crate::routes::Routes;

/// Menu state (open or close), used to toggle the menu
///
enum MenuState {
    Open,
    Close,
}

impl Display for MenuState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MenuState::Open => write!(f, "menu-open"),
            MenuState::Close => write!(f, "menu-close"),
        }
    }
}

/// Layout with two panes. One for the menu and one for the main content
pub fn TwoPanes() -> Element {
    let mut menu_state = use_signal(|| MenuState::Close);

    let close_menu_fn = move |_| menu_state.set(MenuState::Close);
    let toggle_menu_fn = move |_| {
        let new_state = match *menu_state.read() {
            MenuState::Open => MenuState::Close,
            MenuState::Close => MenuState::Open,
        };
        menu_state.set(new_state);
    };

    rsx! {
        div { class: "layout-two-panes container-fluid {menu_state}",
            aside { class: "pane-menu col-12 col-lg-2 position-sticky d-flex flex-column vh-100",
                header { class: "d-flex align-items-center p-3",

                    Link {
                        to: Routes::Status {},
                        class: "navbar-brand d-flex align-items-center",
                        onclick: close_menu_fn,
                        img {
                            src: asset!("assets/images/logo.png"),
                            alt: "MaCPepDB logo",
                        }
                        span { "MaCPepDB" }
                    }
                    button {
                        r#type: "button",
                        class: "btn btn-sm btn-outline-primary d-lg-none",
                        onclick: toggle_menu_fn,
                        i {
                            class: match *menu_state.read() {
                                MenuState::Open => "fa-solid fa-xmark",
                                MenuState::Close => "fa-solid fa-bars",
                            },
                        }
                    }
                }
                div { class: "application-menu d-flex flex-column flex-fill",
                    nav { class: "internal-pages flex-fill px-3",
                        div { class: "separator fw-bold", "Explore the database" }
                        ul { class: "navbar-nav mx-3",
                            li { class: "nav-item",
                                Link {
                                    to: Routes::ProteinSearch {},
                                    onclick: close_menu_fn,
                                    class: "nav-link",
                                    i { class: "fa-solid fa-magnifying-glass me-2" }
                                    "Search proteins"
                                }
                            }
                            li { class: "nav-item",
                                Link {
                                    to: Routes::PeptideSearch {},
                                    onclick: close_menu_fn,
                                    class: "nav-link",
                                    i { class: "fa-solid fa-magnifying-glass me-2" }
                                    "Search peptides"
                                }
                            }
                        }
                        div { class: "separator fw-bold", "Publications" }
                        ul { class: "navbar-nav mx-3",
                            li { class: "nav-item",
                                a {
                                    class: "nav-link",
                                    href: "https://doi.org/10.1021/acs.jproteome.0c00967",
                                    target: "_blank",
                                    "DOI: 10.1021/acs.jproteome.0c00967"
                                    i { class: "fa-solid fa-external-link-alt ms-2" }
                                }
                            }
                        }
                    }
                    nav { class: "external-pages p-3",
                        div { class: "row mb-3",
                            div { class: "col-4 offset-1",
                                a {
                                    href: "https://www.medbioinf.ruhr-uni-bochum.de/",
                                    target: "_blank",
                                    img {
                                        class: "w-100",
                                        src: asset!("assets/images/medbioinf_logo.png"),
                                        alt: "Medical Bioinformatics logo",
                                    }
                                }
                            }
                            div { class: "col-4 offset-1",
                                a {
                                    href: "https://www.mpc.ruhr-uni-bochum.de/",
                                    target: "_blank",
                                    img {
                                        class: "w-100",
                                        src: asset!("assets/images/mpc_logo.png"),
                                        alt: "Medizinisches Proteom-Center logo",
                                    }
                                }
                            }
                        }
                        div { class: "row align-items-center",
                            div { class: "col-4",
                                a {
                                    href: "https://www.cubimed.ruhr-uni-bochum.de",
                                    target: "_blank",
                                    img {
                                        class: "w-100",
                                        src: asset!("assets/images/cubimed_logo.png"),
                                        alt: "CUBiMed.RUB logo",
                                    }
                                }
                            }
                            div { class: "col-4",
                                a {
                                    href: "https://www.medizin.ruhr-uni-bochum.de/index.html.de",
                                    target: "_blank",
                                    img {
                                        class: "w-100",
                                        src: asset!("assets/images/med_faculty_logo.png"),
                                        alt: "Medical Faculty logo",
                                    }
                                }
                            }
                            div { class: "col-4",
                                a {
                                    href: "https://www.ruhr-uni-bochum.de/",
                                    target: "_blank",
                                    img {
                                        class: "w-100",
                                        src: asset!("assets/images/rub_logo.jpg"),
                                        alt: "Ruhr-University Bochum logo",
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "pane-content col-12 col-lg-10 p-3", Outlet::<Routes> {} }
        }
    }
}
