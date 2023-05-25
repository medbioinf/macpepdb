CREATE TABLE macpep.peptides (                                                                                                     [15/63]
    partition smallint,                                                                                                                   
    mass bigint,                                                                                                                          
    sequence text,                                                                                                                        
    a_count smallint,                                                                                                                     
    b_count smallint,                                                                                                                     
    c_count smallint,                                                                                                                     
    c_terminus smallint,                                                                                                                  
    d_count smallint,                                                                                                                     
    e_count smallint,                                                                                                                     
    f_count smallint,                                                                                                                     
    g_count smallint,                                                                                                                     
    h_count smallint,                                                                                                                     
    i_count smallint,                                                                                                                     
    j_count smallint,                                                                                                                     
    k_count smallint,                                                                                                                     
    l_count smallint,
    length smallint,
    m_count smallint,
    n_count smallint,
    n_terminus smallint,
    number_of_missed_cleavages smallint,
    o_count smallint,
    p_count smallint,
    proteins set<text>,
    q_count smallint,
    r_count smallint,
    s_count smallint,
    t_count smallint,
    u_count smallint,
    v_count smallint,
    w_count smallint,
    y_count smallint,
    z_count smallint,
    PRIMARY KEY (partition, mass, sequence)
) WITH CLUSTERING ORDER BY (mass ASC, sequence ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';