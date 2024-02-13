describe 'database' do
    before do
        'rm -rf test.db'
    end
    
    def run_script(commands)
        raw_output = nil
        # open pipe subprocess with initial command, and read/write access, returns pipe
        IO.popen("./meinsql test.db --no-color", "r+") do |pipe|
            commands.each do |command|
                pipe.puts command
            end

            pipe.close_write # close stdin pipe, TODO: understand why this is needed (otherwise pipe.gets doesn't work)

            # from docs: "A separator of nil reads the entire contents"
            raw_output = pipe.gets(nil)
        end

        # returns:
        raw_output.split("\n")
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

    it 'prints error message when table is full' do
        script = (1..1401).map do |i|
            # returns...
            "insert #{i} user#{i} user#{i}@example.com"
        end
        script << ".exit"

        result = run_script(script)
        # `to eq` not to `eqUAL`!!
        expect(result[-2]).to eq "db > table is full"
    end
1
    it 'allows inserting strings that are the maximum length' do
        user = "a"*32
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
            "user1 user1@example.com",
            "executed",
            "exiting",
        ])
    end
end
