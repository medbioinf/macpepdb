use dioxus::prelude::*;

use crate::components::protein_list::SortDirection;

/// Properties for [`Pager`]
///
#[derive(Clone, PartialEq, Props)]
pub struct PagerProps {
    /// Zero-based current page
    pub page: Signal<usize>,
    /// Total number of pages
    pub page_count: usize,
}

/// First/previous/page input/next/last controls. Renders nothing for a single page.
///
#[component]
pub fn Pager(props: PagerProps) -> Element {
    let mut page = props.page;
    let page_count = props.page_count;
    if page_count <= 1 {
        return rsx! {};
    }
    let current_page = page().min(page_count - 1);
    rsx! {
        div { class: "row",
            div { class: "col-12 col-md-8 col-lg-6",
                div { class: "input-group mb-3",
                    button {
                        class: "btn btn-primary",
                        r#type: "button",
                        disabled: current_page == 0,
                        onclick: move |_| page.set(0),
                        i { class: "fa fa-chevron-left" }
                        i { class: "fa fa-chevron-left" }
                    }
                    button {
                        class: "btn btn-primary",
                        r#type: "button",
                        disabled: current_page == 0,
                        onclick: move |_| page.set(current_page.saturating_sub(1)),
                        i { class: "fa fa-chevron-left" }
                    }
                    input {
                        class: "form-control",
                        r#type: "number",
                        step: 1,
                        min: 1,
                        max: page_count,
                        value: current_page + 1,
                        onchange: move |evt| {
                            if let Ok(requested) = evt.value().parse::<usize>() {
                                page.set(requested.clamp(1, page_count) - 1);
                            }
                        },
                    }
                    span { class: "input-group-text", "/ {page_count}" }
                    button {
                        class: "btn btn-primary",
                        r#type: "button",
                        disabled: current_page + 1 >= page_count,
                        onclick: move |_| page.set(current_page + 1),
                        i { class: "fa fa-chevron-right" }
                    }
                    button {
                        class: "btn btn-primary",
                        r#type: "button",
                        disabled: current_page + 1 >= page_count,
                        onclick: move |_| page.set(page_count - 1),
                        i { class: "fa fa-chevron-right" }
                        i { class: "fa fa-chevron-right" }
                    }
                }
            }
        }
    }
}

/// Properties for [`PageSizeSelect`]
///
#[derive(Clone, PartialEq, Props)]
pub struct PageSizeSelectProps {
    /// HTML id of the select, must be unique on the page
    pub id: String,
    /// Label shown next to the select
    pub label: String,
    /// Currently selected page size
    pub page_size: Signal<usize>,
    /// Current page, reset to the first page on change
    pub page: Signal<usize>,
    /// Selectable sizes
    pub options: Vec<usize>,
}

/// Select for the number of rows per page
///
#[component]
pub fn PageSizeSelect(props: PageSizeSelectProps) -> Element {
    let mut page_size = props.page_size;
    let mut page = props.page;
    let size = page_size();
    rsx! {
        div { class: "d-flex align-items-center gap-2",
            label { r#for: "{props.id}", class: "mb-0", "{props.label}" }
            select {
                id: "{props.id}",
                class: "form-select w-auto",
                value: "{size}",
                onchange: move |evt| {
                    if let Ok(new_size) = evt.value().parse::<usize>() {
                        page_size.set(new_size);
                        page.set(0);
                    }
                },
                for option in props.options.iter().copied() {
                    option { value: "{option}", selected: option == size, "{option}" }
                }
            }
        }
    }
}

/// Table header cell which toggles sorting on click, with a sort indicator if `direction`
/// is set (i.e. this column is the active one).
///
pub fn sortable_th(
    label: &'static str,
    direction: Option<SortDirection>,
    on_click: EventHandler<()>,
) -> Element {
    let icon = match direction {
        Some(SortDirection::Ascending) => "fa-sort-up",
        Some(SortDirection::Descending) => "fa-sort-down",
        None => "fa-sort",
    };
    rsx! {
        th {
            style: "cursor: pointer; user-select: none",
            onclick: move |_| on_click.call(()),
            "{label} "
            i { class: "fas {icon}" }
        }
    }
}
