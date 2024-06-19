describe 'database' do
    before do
        # backticks: runs given command - ruby syntax
        `rm test.db`
    end

    after(:all) do
        # runs after all tasks are done (e.g. after last task)
        `rm test.db`
    end

    def run_script(commands)
        raw_output = nil

        # open pipe subprocess with initial command, and read/write access, returns pipe
        IO.popen("./meinsql test.db --no-color", "r+") do |pipe|
            commands.each do |command|
                begin
                    pipe.puts command
                rescue
                    break
                end
            end

            pipe.close_write # close stdin pipe, RESEARCH: understand why this is needed (otherwise pipe.gets doesn't work)

            # from docs: "A separator of nil reads the entire contents"
            raw_output = pipe.gets(nil)
        end

        # returns:
        raw_output.split("\n") # if raw_output is nil here, program likely crashed (e.g. segfault)
    end

    it 'inserts and retrieves a row' do
        result = run_script([
            "insert 1 user1 user1@example.com",
            "select",
            ".exit"
        ])
        expect(result).to match_array([
            "db > executed",
            "db > 1 user1 user1@example.com",
            "executed",
            "db > exiting"
        ])
    end

    # TODO: how damn big is this table?
    it 'prints error message when table is full' do
        script = (1..700).map do |i|
            # returns...al
            "insert #{i} user#{i} user#{i}@example.com"
        end
        script << ".exit"

        result = run_script(script)

        expect(result[-1]).to match "db > src/common.h:179: tried to fetch a page number larger than max. allowed: 100 > 100"
    end

    it 'allows inserting strings that are the maximum length' do
        user = "a"*31
        email = "a"*255

        script = [
            "insert 1 #{user} #{email}",
            "select",
            ".exit",
        ]
        result = run_script(script)

        expect(result[-3]).to eq "db > 1 #{user} #{email}"
    end

    it 'disallows inserting strings larger than the maximum length' do
        user = "a"*32
        email = "a"*(255+1)

        script = [
            "insert 1 #{user} #{email}",
            "select",
            ".exit",
        ]
        result = run_script(script)

        expect(result[-3]).to eq "db > string too long for command: insert"
    end

    it 'keeps data after closing connection' do
        result1 = run_script([
            "insert 1 user1 user1@example.com",
            ".exit",
        ])

        result2 = run_script([
            "select",
            ".exit",
        ])

        expect(result2).to match_array([
            "db > 1 user1 user1@example.com",
            "db > exiting",
            "executed",
        ])
    end

    it 'prints constants' do
        result = run_script([
            ".print",
            ".exit"
        ])
        expect(result).to match_array [
            "db > constants:",
            # "ROW_SIZE: 293",
            # "COMMON_NODE_HEADER_SIZE: 6",
            # "INTERNAL_NODE_MAX_KEYS: 510",
            # "LEAF_NODE_HEADER_SIZE: 14",
            # "LEAF_NODE_CELL_SIZE: 297",
            # "LEAF_NODE_SPACE_FOR_CELLS: 4082",
            # "LEAF_NODE_MAX_CELLS: 13",
            "ROW_SIZE: 292",
            "COMMON_NODE_HEADER_SIZE: 12",
            "INTERNAL_NODE_MAX_KEYS: 509",
            "LEAF_NODE_HEADER_SIZE: 20",
            "LEAF_NODE_CELL_SIZE: 292",
            "LEAF_NODE_SPACE_FOR_CELLS: 4076",
            "LEAF_NODE_MAX_CELLS: 13",
            "db > exiting",
        ]
    end

    it 'prints the structure of a one-node btree' do
        script = [3, 1, 2].map do |i|
            "insert #{i} user#{i} user#{i}@example.com"
        end

        script << ".btree"
        script << ".exit"

        result = run_script(script)

        expect(result).to match_array([
            "db > executed",
            "db > executed",
            "db > executed",
            "db > page 0/100; root; leaf; 3/13 keys",
            "  - key 1",
            "  - key 2",
            "  - key 3",
            "db > exiting",
        ])
    end

    it 'prints an error message if there is a duplicate key' do
        script = [
            'insert 1 user1 user1@example.com',
            'insert 1 user2 user2@example.com',
            '.exit'
        ]

        result = run_script(script)
        expect(result).to match_array([
            "db > executed",
            "db > failed to execute statement: duplicate key: 1",
            "db > exiting",
        ])
    end

    it "prints all rows in a multi-level tree" do
        script = (1..15).map do |i|
            "insert #{i} user#{i} user#{i}@example.com"
        end
        script << "select"
        script << ".exit"

        result = run_script(script)

        expect(result).to match_array([
            *(1..15).map do |i|
                "db > executed"
            end,
            "db > 1 user1 user1@example.com",
            *(2..15).map do |i|
                "#{i} user#{i} user#{i}@example.com"
            end,

            # "db > executed",
            "executed",
            "db > exiting",
        ])
    end
end
