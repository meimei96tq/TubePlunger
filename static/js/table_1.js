$(document).ready(function () {
    $('#dataTableRight').DataTable({
        language: {
            searchBuilder: {
                title: ''
            }
        },
        dom: 'QfBrtipRS',
        buttons: [
            'csv', 'excel', 'pdf'
        ],
        // responsive: true,
        fixedHeader: true,
        colReorder: true,
    });
});
